load('Tiltfile.opencost', 'run_opencost')

# WARNING: this allows any k8s context for deployment
# allow_k8s_contexts(k8s_context())
# To allow a specific context for deployment:
# allow_k8s_contexts('kubectl-context')
# See https://docs.tilt.dev/api.html#api.allow_k8s_contexts for default
# allowed contexts

config.define_string('arch')
config.define_string('cloud-integration')
config.define_bool('delve-continue')
config.define_string('docker-repo')
config.define_string('helm-values')
config.define_string('port-costmodel')
config.define_string('port-debug')
config.define_string('port-prometheus')
config.define_string('port-ui')
config.define_string('service-key')
cfg = config.parse()

docker_repo = cfg.get('docker-repo', '')
if docker_repo != '':
    docker_repo += "/"

port_costmodel = cfg.get('port-costmodel', 9003)
port_debug = cfg.get('port-debug', 40000)
port_prometheus = cfg.get('port-prometheus', 9080)
port_ui = cfg.get('port-ui', 9090)

options = {
    'arch': cfg.get('arch'),
    'cloud_integration': cfg.get('cloud-integration', ''),
    'delve_continue': cfg.get('delve-continue', True),
    'docker_repo': docker_repo,
    'helm_values': cfg.get('helm-values', './tilt-values.yaml'),
    'port_costmodel': cfg.get('port-costmodel', '9003'),
    'port_debug': cfg.get('port-debug', '40000'),
    'port_prometheus': cfg.get('port-prometheus', '9080'),
    'port_ui': cfg.get('port-ui', '9090'),
    'service_key': cfg.get('service-key', ''),
}

run_opencost(options)
