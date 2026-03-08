{{/*
Expand the name of the chart.
*/}}
{{- define "retail-pipeline.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to all resources.
*/}}
{{- define "retail-pipeline.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Selector labels for a given component.
Usage: include "retail-pipeline.selectorLabels" (dict "root" . "component" "kafka")
*/}}
{{- define "retail-pipeline.selectorLabels" -}}
app.kubernetes.io/name: {{ include "retail-pipeline.name" .root }}
app.kubernetes.io/component: {{ .component }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end }}
