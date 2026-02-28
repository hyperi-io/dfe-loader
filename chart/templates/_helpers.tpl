{{/*
Expand the name of the chart.
*/}}
{{- define "dfe-loader.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated at 63 chars because some K8s name fields are limited.
*/}}
{{- define "dfe-loader.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "dfe-loader.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "dfe-loader.labels" -}}
helm.sh/chart: {{ include "dfe-loader.chart" . }}
{{ include "dfe-loader.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "dfe-loader.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dfe-loader.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "dfe-loader.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dfe-loader.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Kafka secret name — use existing or generate from fullname.
*/}}
{{- define "dfe-loader.kafkaSecretName" -}}
{{- if .Values.kafka.existingSecret }}
{{- .Values.kafka.existingSecret }}
{{- else }}
{{- printf "%s-kafka" (include "dfe-loader.fullname" .) }}
{{- end }}
{{- end }}

{{/*
ClickHouse secret name — use existing or generate from fullname.
*/}}
{{- define "dfe-loader.clickhouseSecretName" -}}
{{- if .Values.clickhouse.existingSecret }}
{{- .Values.clickhouse.existingSecret }}
{{- else }}
{{- printf "%s-clickhouse" (include "dfe-loader.fullname" .) }}
{{- end }}
{{- end }}
