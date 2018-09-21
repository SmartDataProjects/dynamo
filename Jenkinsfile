pipeline {
    agent {
        dockerfile {
            args '-u root:root'
        }
    }
    stages {
        stage ('installation') {
            steps {
                sh 'cp defaults.json.template defaults.json'
                sh 'cp dynamo.cfg.template dynamo.cfg'
                sh 'cp mysql/grants.json.template mysql/grants.json'
                sh 'mysqld_safe &'
                sh 'sleep 5'    // Wait for daemon to start
                sh 'printf "test\n" | ./install.sh'
                sh "python -c 'from dynamo.core.executable import inventory; print inventory.sites'"
            }
        }
    }
}
