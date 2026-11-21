"use client";

export interface ThroughputPoint {
  /** ISO bucket label. */
  label: string;
  /** Executions started in the bucket. */
  started: number;
  /** Executions finished in the bucket. */
  finished: number;
}

export interface ThroughputChartProps {
  points: readonly ThroughputPoint[];
  width?: number;
  height?: number;
}

/**
 * Dependency-free SVG throughput chart (started vs finished per bucket).
 * Renders axes, gridlines and two real series — the shape comes from the
 * data, empty series render as an explicit "no data" panel.
 */
export function ThroughputChart({ points, width = 640, height = 220 }: ThroughputChartProps) {
  if (points.length === 0) {
    return (
      <div className="mas-chart-empty" role="status">
        No throughput data yet.
      </div>
    );
  }
  const padding = 32;
  const innerWidth = width - padding * 2;
  const innerHeight = height - padding * 2;
  const max = Math.max(1, ...points.map((point) => Math.max(point.started, point.finished)));
  const step = innerWidth / points.length;
  const barWidth = Math.max(2, step / 2.4);
  const scaleY = (value: number) => innerHeight - (value / max) * innerHeight;

  return (
    <svg
      className="mas-chart"
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label="Execution throughput"
      width="100%"
      height={height}
    >
      <g transform={`translate(${padding}, ${padding})`}>
        {[0, 0.25, 0.5, 0.75, 1].map((fraction) => (
          <line
            key={fraction}
            x1={0}
            x2={innerWidth}
            y1={innerHeight * fraction}
            y2={innerHeight * fraction}
            className="mas-chart-grid"
          />
        ))}
        {points.map((point, index) => {
          const x = index * step + step / 2;
          return (
            <g key={point.label}>
              <rect
                x={x - barWidth}
                y={scaleY(point.started)}
                width={barWidth}
                height={innerHeight - scaleY(point.started)}
                className="mas-chart-bar-started"
              >
                <title>{`${point.label}: ${point.started} started`}</title>
              </rect>
              <rect
                x={x}
                y={scaleY(point.finished)}
                width={barWidth}
                height={innerHeight - scaleY(point.finished)}
                className="mas-chart-bar-finished"
              >
                <title>{`${point.label}: ${point.finished} finished`}</title>
              </rect>
            </g>
          );
        })}
        <line x1={0} x2={innerWidth} y1={innerHeight} y2={innerHeight} className="mas-chart-axis" />
      </g>
    </svg>
  );
}
