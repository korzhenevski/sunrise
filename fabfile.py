from fabric.api import run, local, lcd, put, env

env.hosts = ['root@de5']

def deploy_tracker():
	with lcd('./tracker'):
		local('go build tracker.go')
		put('tracker', '/usr/local/bin/sunrise-tracker')
	run('restart sunrise-tracker')
