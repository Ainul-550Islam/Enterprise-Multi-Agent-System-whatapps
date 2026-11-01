//! Cardinality-guarded metrics registry with Prometheus-compatible text
//! exposition.
//!
//! The entire risk profile of metrics systems is cardinality explosions —
//! a `user_id` label can OOM a collector. Rule: labels are *dimensions*,
//! never *identifiers*. The registry enforces hard structural caps (label
//! count, key/value lengths, series per family) and counts dropped series
//! in a dedicated counter so regressions are themselves observable.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

/// Maximum labels per series.
pub const MAX_LABELS: usize = 10;
/// Maximum series per metric family (cardinality fuse).
pub const MAX_SERIES_PER_FAMILY: usize = 10_000;
/// Internal counter tracking dropped series due to the cardinality fuse.
pub const DROPPED_SERIES_METRIC: &str = "mas_metrics_cardinality_dropped_total";

/// Validates a metric family name (`[a-z][a-z0-9_:]*`, ≤ 128).
pub fn validate_family(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | ':'));
    if ok {
        Ok(())
    } else {
        Err(AppError::invalid_field(
            "metric",
            "invalid_name",
            format!("metric names match [a-z][a-z0-9_:]* (≤128), got {name:?}"),
        ))
    }
}

/// An ordered, deduplicated label set for one series.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelSet(Vec<(String, String)>);

impl LabelSet {
    /// Empty label set.
    #[must_use]
    pub const fn empty() -> Self {
        Self(Vec::new())
    }

    /// Builds a validated, sorted set. Duplicate keys are rejected to keep
    /// exposition unambiguous.
    pub fn build<I, K, V>(labels: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut pairs: Vec<(String, String)> = labels
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        if pairs.len() > MAX_LABELS {
            return Err(AppError::invalid_field(
                "labels",
                "too_many",
                format!("at most {MAX_LABELS} labels per series"),
            ));
        }
        pairs.sort();
        for window in pairs.windows(2) {
            if window[0].0 == window[1].0 {
                return Err(AppError::invalid_field(
                    "labels",
                    "duplicate_key",
                    format!("duplicate label key {:?}", window[0].0),
                ));
            }
        }
        for (key, value) in &pairs {
            let key_ok = !key.is_empty()
                && key.len() <= 64
                && key.starts_with(|c: char| c.is_ascii_lowercase())
                && key
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if !key_ok {
                return Err(AppError::invalid_field(
                    "labels",
                    "invalid_key",
                    format!("label keys match [a-z][a-z0-9_]* (≤64), got {key:?}"),
                ));
            }
            let value_ok = value.len() <= 128 && !value.chars().any(char::is_control);
            if !value_ok {
                return Err(AppError::invalid_field(
                    "labels",
                    "invalid_value",
                    "label values are ≤128 chars without control characters",
                ));
            }
        }
        Ok(Self(pairs))
    }

    /// Number of labels.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no labels.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn escaped(value: &str) -> String {
        value.replace('\\', "\\\\").replace('"', "\\\"")
    }
}

impl fmt::Display for LabelSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }
        f.write_str("{")?;
        for (index, (key, value)) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{key}=\"{}\"", Self::escaped(value))?;
        }
        f.write_str("}")
    }
}

/// Metric family kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// Monotonically increasing counter.
    Counter,
    /// Point-in-time gauge.
    Gauge,
    /// Bucketed distribution.
    Histogram,
}

impl MetricKind {
    const fn prometheus_type(&self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

/// Default latency histogram buckets, in milliseconds.
pub const DEFAULT_BUCKETS_MS: [f64; 11] = [
    1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
];

#[derive(Debug, Clone)]
enum SeriesValue {
    Counter(u64),
    Gauge(i64),
    Histogram {
        buckets: Vec<f64>,
        counts: Vec<u64>,
        sum: f64,
        count: u64,
    },
}

#[derive(Debug, Clone, Copy)]
struct FamilyMeta {
    kind: MetricKind,
}

impl FamilyMeta {
    const fn new(kind: MetricKind) -> Self {
        Self { kind }
    }
}

/// Thread-safe metrics registry.
#[derive(Debug)]
pub struct MetricsRegistry {
    series: Mutex<BTreeMap<(String, LabelSet), SeriesValue>>,
    families: Mutex<BTreeMap<String, FamilyMeta>>,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self {
            series: Mutex::new(BTreeMap::new()),
            families: Mutex::new(BTreeMap::new()),
        }
    }
}

impl MetricsRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn declare(&self, name: &str, kind: MetricKind) -> Result<()> {
        validate_family(name)?;
        let mut families = self.families.lock().unwrap_or_else(|e| e.into_inner());
        match families.get(name) {
            Some(meta) if meta.kind != kind => Err(AppError::conflict(format!(
                "metric family '{name}' already registered as {:?}",
                meta.kind
            ))),
            Some(_) => Ok(()),
            None => {
                families.insert(name.to_owned(), FamilyMeta::new(kind));
                Ok(())
            },
        }
    }

    /// Returns `false` (and counts the drop) when the family is saturated.
    fn admit_series(
        &self,
        name: &str,
        labels: &LabelSet,
        kind: MetricKind,
        value: SeriesValue,
    ) -> Result<bool> {
        let mut series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        let key = (name.to_owned(), labels.clone());
        if series.contains_key(&key) {
            return Ok(true);
        }
        let family_size = series
            .range((name.to_owned(), LabelSet::empty())..)
            .take_while(|((family, _), _)| family == name)
            .count();
        if family_size >= MAX_SERIES_PER_FAMILY {
            let _ = (kind, value); // saturated: discard the new series
            let dropped = series
                .entry((DROPPED_SERIES_METRIC.to_owned(), LabelSet::empty()))
                .or_insert_with(|| SeriesValue::Counter(0));
            if let SeriesValue::Counter(total) = dropped {
                *total = total.saturating_add(1);
            }
            return Ok(false);
        }
        series.insert(key, value);
        Ok(true)
    }

    /// Increments a counter family (created on first use).
    /// Returns `false` only when the cardinality fuse tripped.
    pub fn inc_counter(&self, name: &str, labels: &LabelSet, delta: u64) -> Result<bool> {
        self.declare(name, MetricKind::Counter)?;
        if !self.admit_series(name, labels, MetricKind::Counter, SeriesValue::Counter(0))? {
            return Ok(false);
        }
        let mut series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        let value = series
            .entry((name.to_owned(), labels.clone()))
            .or_insert_with(|| SeriesValue::Counter(0));
        match value {
            SeriesValue::Counter(total) => {
                *total = total.saturating_add(delta);
                Ok(true)
            },
            _ => Err(AppError::internal(
                "metric kind drifted under the registry lock",
            )),
        }
    }

    /// Sets a gauge family (created on first use).
    pub fn set_gauge(&self, name: &str, labels: &LabelSet, value: i64) -> Result<bool> {
        self.declare(name, MetricKind::Gauge)?;
        if !self.admit_series(name, labels, MetricKind::Gauge, SeriesValue::Gauge(0))? {
            return Ok(false);
        }
        let mut series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        let entry = series
            .entry((name.to_owned(), labels.clone()))
            .or_insert_with(|| SeriesValue::Gauge(0));
        match entry {
            SeriesValue::Gauge(current) => {
                *current = value;
                Ok(true)
            },
            _ => Err(AppError::internal(
                "metric kind drifted under the registry lock",
            )),
        }
    }

    /// Records one histogram observation (milliseconds), created on first
    /// use with [`DEFAULT_BUCKETS_MS`].
    pub fn observe_histogram(&self, name: &str, labels: &LabelSet, value_ms: f64) -> Result<bool> {
        if !value_ms.is_finite() || value_ms < 0.0 {
            return Err(AppError::invalid_field(
                "value",
                "invalid",
                "histogram observations must be finite and non-negative",
            ));
        }
        self.declare(name, MetricKind::Histogram)?;
        let fresh = || SeriesValue::Histogram {
            buckets: DEFAULT_BUCKETS_MS.to_vec(),
            counts: vec![0; DEFAULT_BUCKETS_MS.len()],
            sum: 0.0,
            count: 0,
        };
        if !self.admit_series(name, labels, MetricKind::Histogram, fresh())? {
            return Ok(false);
        }
        let mut series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        let entry = series
            .entry((name.to_owned(), labels.clone()))
            .or_insert_with(fresh);
        match entry {
            SeriesValue::Histogram {
                buckets,
                counts,
                sum,
                count,
            } => {
                for (index, boundary) in buckets.iter().enumerate() {
                    if value_ms <= *boundary {
                        counts[index] += 1;
                    }
                }
                *sum += value_ms;
                *count += 1;
                Ok(true)
            },
            _ => Err(AppError::internal(
                "metric kind drifted under the registry lock",
            )),
        }
    }

    /// Number of registered families.
    #[must_use]
    pub fn family_count(&self) -> usize {
        self.families
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Number of stored series (excludes the internal drop counter when
    /// empty).
    #[must_use]
    pub fn series_count(&self) -> usize {
        self.series.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Current counter value for one exact series (0 when absent).
    #[must_use]
    pub fn counter_value(&self, name: &str, labels: &LabelSet) -> u64 {
        match self
            .series
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(name.to_owned(), labels.clone()))
        {
            Some(SeriesValue::Counter(total)) => *total,
            _ => 0,
        }
    }

    /// Current gauge value for one exact series (0 when absent).
    #[must_use]
    pub fn gauge_value(&self, name: &str, labels: &LabelSet) -> i64 {
        match self
            .series
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(name.to_owned(), labels.clone()))
        {
            Some(SeriesValue::Gauge(current)) => *current,
            _ => 0,
        }
    }

    /// Renders the registry as Prometheus text exposition format.
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let families = self
            .families
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let series = self
            .series
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut out = String::new();

        // The drop counter rides along even when no family declared it.
        let dropped = series
            .get(&(DROPPED_SERIES_METRIC.to_owned(), LabelSet::empty()))
            .and_then(|v| match v {
                SeriesValue::Counter(total) => Some(*total),
                _ => None,
            });
        if let Some(total) = dropped {
            let _ = fmt::Write::write_fmt(
                &mut out,
                format_args!(
                    "# TYPE {DROPPED_SERIES_METRIC} counter\n{DROPPED_SERIES_METRIC} {total}\n"
                ),
            );
        }

        for (family, meta) in &families {
            let _ = fmt::Write::write_fmt(
                &mut out,
                format_args!("# TYPE {family} {}\n", meta.kind.prometheus_type()),
            );
            for ((name, labels), value) in series
                .range((family.clone(), LabelSet::empty())..)
                .take_while(|((owned, _), _)| owned == family)
            {
                match value {
                    SeriesValue::Counter(total) => {
                        let _ = fmt::Write::write_fmt(
                            &mut out,
                            format_args!("{name}{labels} {total}\n"),
                        );
                    },
                    SeriesValue::Gauge(current) => {
                        let _ = fmt::Write::write_fmt(
                            &mut out,
                            format_args!("{name}{labels} {current}\n"),
                        );
                    },
                    SeriesValue::Histogram {
                        buckets,
                        counts,
                        sum,
                        count,
                    } => {
                        for (boundary, bucket_count) in buckets.iter().zip(counts.iter()) {
                            let _ = fmt::Write::write_fmt(
                                &mut out,
                                format_args!("{name}_bucket{{le=\"{boundary}{labels_suffix}\"}} {bucket_count}\n",
                                    boundary = boundary,
                                    labels_suffix = labels_suffix(labels)),
                            );
                        }
                        let _ = fmt::Write::write_fmt(
                            &mut out,
                            format_args!(
                                "{name}_bucket{{le=\"+Inf\"{labels_suffix}}} {count}\n{name}_sum{labels} {sum}\n{name}_count{labels} {count}\n",
                                labels_suffix = labels_suffix(labels)),
                        );
                    },
                }
            }
        }
        out
    }
}

fn labels_suffix(labels: &LabelSet) -> String {
    let text = labels.to_string();
    if text.is_empty() {
        String::new()
    } else {
        // `{k="v"}` → `,k="v"` for appending inside an existing `{…}`.
        let mut suffix = text
            .trim_start_matches('{')
            .trim_end_matches('}')
            .to_owned();
        suffix.insert(0, ',');
        suffix
    }
}

/// Serializes the current registry into a compact JSON snapshot
/// (families + series, values as numbers).
#[must_use]
pub fn snapshot_json(registry: &MetricsRegistry) -> Value {
    let series = registry.series.lock().unwrap_or_else(|e| e.into_inner());
    let mut by_family: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for ((name, labels), value) in series.iter() {
        let mut item = Map::new();
        let label_obj: Value = Value::Object(
            labels
                .0
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect(),
        );
        item.insert("labels".to_owned(), label_obj);
        match value {
            SeriesValue::Counter(total) => {
                item.insert("counter".to_owned(), Value::from(*total));
            },
            SeriesValue::Gauge(current) => {
                item.insert("gauge".to_owned(), Value::from(*current));
            },
            SeriesValue::Histogram { count, sum, .. } => {
                item.insert("count".to_owned(), Value::from(*count));
                item.insert("sum_ms".to_owned(), Value::from(*sum));
            },
        }
        by_family
            .entry(name.clone())
            .or_default()
            .push(Value::Object(item));
    }
    Value::Object(
        by_family
            .into_iter()
            .map(|(k, v)| (k, Value::Array(v)))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_labels_are_validated() {
        assert!(validate_family("mas_requests_total").is_ok());
        assert!(validate_family("mas:http_requests").is_ok());
        assert!(validate_family("Mas_upper").is_err());
        assert!(validate_family("1leading_digit").is_err());
        assert!(validate_family("").is_err());

        let labels = LabelSet::build([("method", "get"), ("route", "/v1/agents")]).expect("labels");
        assert_eq!(labels.to_string(), "{method=\"get\",route=\"/v1/agents\"}");
        assert!(
            LabelSet::build([("method", "get"), ("method", "post")]).is_err(),
            "duplicate keys reject"
        );
        assert!(LabelSet::build([("Bad-Key", "x")]).is_err());
        assert!(
            LabelSet::build([("k", "line\nbreak")]).is_err(),
            "control chars reject"
        );
        let escaped = LabelSet::build([("note", "say \"hi\" \\ ok")]).expect("labels");
        assert_eq!(escaped.to_string(), "{note=\"say \\\"hi\\\" \\\\ ok\"}");
    }

    #[test]
    fn counters_gauges_and_kind_conflicts_behave() {
        let registry = MetricsRegistry::new();
        let none = LabelSet::empty();
        assert!(registry
            .inc_counter("mas_runs_total", &none, 1)
            .expect("inc"));
        assert!(registry
            .inc_counter("mas_runs_total", &none, 41)
            .expect("inc"));
        assert_eq!(registry.counter_value("mas_runs_total", &none), 42);

        let route = LabelSet::build([("route", "/x")]).expect("labels");
        assert!(registry.set_gauge("mas_inflight", &route, 3).expect("set"));
        assert!(registry.set_gauge("mas_inflight", &route, 7).expect("set"));
        assert_eq!(registry.gauge_value("mas_inflight", &route), 7);

        assert!(
            registry.inc_counter("mas_inflight", &route, 1).is_err(),
            "same family name with a different kind conflicts"
        );
        assert!(registry.gauge_value("ghost", &none) == 0);
    }

    #[test]
    fn histograms_bucket_observations_and_render_prometheus() {
        let registry = MetricsRegistry::new();
        let none = LabelSet::empty();
        registry
            .observe_histogram("mas_latency_ms", &none, 4.0)
            .expect("obs");
        registry
            .observe_histogram("mas_latency_ms", &none, 75.0)
            .expect("obs");
        registry
            .observe_histogram("mas_latency_ms", &none, 9_000.0)
            .expect("obs");
        assert!(registry
            .observe_histogram("mas_latency_ms", &none, f64::NAN)
            .is_err());

        let rendered = registry.render_prometheus();
        assert!(rendered.contains("# TYPE mas_latency_ms histogram"));
        assert!(rendered.contains("mas_latency_ms_bucket{le=\"5\"} 1"));
        assert!(rendered.contains("mas_latency_ms_bucket{le=\"100\"} 2"));
        assert!(rendered.contains("mas_latency_ms_bucket{le=\"+Inf\"} 3"));
        assert!(rendered.contains("mas_latency_ms_count 3"));
        assert!(rendered.contains("mas_latency_ms_sum 9079"));
    }

    #[test]
    fn snapshot_and_render_cover_all_series() {
        let registry = MetricsRegistry::new();
        let get = LabelSet::build([("method", "get")]).expect("labels");
        registry
            .inc_counter("mas_requests_total", &get, 5)
            .expect("inc");
        registry
            .set_gauge("mas_pool_open", &LabelSet::empty(), 12)
            .expect("set");
        let snap = snapshot_json(&registry);
        assert_eq!(snap["mas_requests_total"][0]["counter"], Value::from(5));
        assert_eq!(
            snap["mas_requests_total"][0]["labels"]["method"],
            Value::from("get")
        );
        assert_eq!(snap["mas_pool_open"][0]["gauge"], Value::from(12));

        let rendered = registry.render_prometheus();
        assert!(rendered.contains("mas_requests_total{method=\"get\"} 5"));
        assert!(rendered.contains("mas_pool_open 12"));
    }
}
