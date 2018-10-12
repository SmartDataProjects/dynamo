pipeline {
    agent {

        dockerfile {
            // Run as root user inside container
            // Mount ~/public_html for copying coverage page over
            args '-u root:root -v $HOME/public_html:/html'
        }

    }

    stages {

        stage ('Setup Server') {
            steps {
                // Start MySQL
                sh '''
                   mysqld_safe &
                   sleep 2
                   '''

                sh '''
                   /etc/pki/tls/certs/make-dummy-cert /etc/pki/tls/certs/localhost.crt
                   chmod +r /etc/pki/tls/certs/localhost.crt

                   # Create a certificate for dynamo user
                   printf "US\nMass\nBahston\nDynamo\ntest\nlocalhost\n\n" | \
                       openssl req -new -newkey rsa:1024 -days 365 -nodes -x509 -keyout /tmp/x509up_u500 -out /tmp/x509up_u500
                   chown dynamo:dynamo /tmp/x509up_u500

                   # Add certificates to trusted
                   cp /tmp/x509up_u500 /etc/pki/tls/certs/localhost.crt /etc/pki/ca-trust/source/anchors/
                   update-ca-trust extract
                   '''

                // Lighttpd files
                sh '''
                   sed 's/__ipaddr__/0.0.0.0/g' web/lighttpd/lighttpd.conf > /etc/lighttpd/lighttpd.conf

                   yes | cp web/lighttpd/modules.conf  /etc/lighttpd/modules.conf

                   sed 's|__socket__|/var/spool/dynamo/dynamoweb.sock|g' web/lighttpd/fastcgi.conf > /etc/lighttpd/conf.d/fastcgi.conf

                   sed 's|__certkeyfile__|/etc/pki/tls/certs/localhost.crt|g' web/lighttpd/ssl.conf | \
                       sed 's|__cafile__|/tmp/x509up_u500|g' | sed 's/    ssl.crl/#    ssl.crl/g' > /etc/lighttpd/conf.d/ssl.conf
                   '''
            }
        }

        stage ('Dynamo Installation') {
            steps {
                // Copy template files to proper location
                sh '''
                   cp defaults.json.template defaults.json
                   cp dynamo.cfg.template dynamo.cfg
                   cp mysql/grants.json.template mysql/grants.json
                   '''

                // Install Dynamo
                sh 'printf "test\n" | ./install.sh'

                // Need environment for whole thing
                sh '''
                   source /usr/local/dynamo/etc/profile.d/init.sh

                   # Create dynamo user
                   yes | dynamo-user-auth --user dynamo --dn "/C=US/ST=Mass/L=Bahston/O=Dynamo/OU=test/CN=localhost" --role admin
                   dynamo-user-auth --user dynamo --role admin --target inventory

                   # Start server
                   dynamod &
                   sleep 3

                   # Authorize applications
                   dynamo-exec-auth -u dynamo -x $PWD/test/dynamo_setup.py --title setup
                   dynamo-exec-auth -u dynamo -x $PWD/test/dynamo_teardown.py --title teardown
                   '''

                 // Start lighttpd
                 sh '''
                    lighttpd -D -f /etc/lighttpd/lighttpd.conf &
                    sleep 2
                    '''
            }
        }

        stage ('Unit Tests') {
            steps {
                sh 'opsspace-test'

                // Look at the results for debugging/confirmation
                sh 'cat /var/log/dynamo/server.log'
            }
        }

//        stage ('Report Results') {
//            steps {
//                sh 'copy-coverage-html /html/coverage/${JOB_NAME}/${BUILD_NUMBER}'
//            }
//        }
    }
}
