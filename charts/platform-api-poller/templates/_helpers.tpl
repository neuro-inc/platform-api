{{- define "platformApiPoller.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "platformApiPoller.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "platformApiPoller.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "platformApiPoller.labels.standard" -}}
app: {{ include "platformApiPoller.name" . }}
chart: {{ include "platformApiPoller.chart" . }}
heritage: {{ .Release.Service | quote }}
release: {{ .Release.Name | quote }}
{{- end -}}

{{- define "platformApiPoller.ingress.apiVersion" -}}
{{- if and (.Capabilities.APIVersions.Has "networking.k8s.io/v1") (semverCompare ">= 1.19-0" .Capabilities.KubeVersion.Version) -}}
{{- print "networking.k8s.io/v1" -}}
{{- else if .Capabilities.APIVersions.Has "networking.k8s.io/v1beta1" -}}
{{- print "networking.k8s.io/v1beta1" -}}
{{- else -}}
{{- print "extensions/v1beta1" -}}
{{- end -}}
{{- end -}}
