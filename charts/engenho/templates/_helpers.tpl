{{/* Common template helpers */}}

{{- define "engenho.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "engenho.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/name: engenho
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
engenho.io/cluster: {{ .Values.global.cluster.name | quote }}
engenho.io/region: {{ .Values.global.cluster.region | quote }}
{{- end -}}

{{- define "engenho.natsURL" -}}
nats://{{ .Release.Name }}-nats:4222
{{- end -}}

{{- define "engenho.image" -}}
{{- $img := index . 0 -}}
{{- $root := index . 1 -}}
{{ $root.Values.global.image.registry }}/{{ $root.Values.global.image.repository }}/{{ $img }}:{{ $root.Values.global.image.tag }}
{{- end -}}
