{{/*
Expand the name of the chart.
*/}}
{{- define "enterprise-mas.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "enterprise-mas.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "enterprise-mas.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "enterprise-mas.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "enterprise-mas.selectorLabels" -}}
app.kubernetes.io/name: {{ include "enterprise-mas.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Container port helper
*/}}
{{- define "enterprise-mas.containerPort" -}}
{{- . -}}
{{- end }}

{{/*
Data-plane egress (PostgreSQL + NATS).

Both are external to this chart, so they are addressed by namespace rather than
by a pod selector in the release namespace. Emitted as raw egress list items so
callers can `include ... | nindent` it directly under `egress:`.
*/}}
{{- define "enterprise-mas.dataEgress" -}}
- to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: {{ .Values.networkPolicy.dataNamespace }}
  ports:
    - port: {{ .Values.postgres.port | default 5432 }}
      protocol: TCP
    - port: {{ .Values.nats.port | default 4222 }}
      protocol: TCP
{{- end }}
