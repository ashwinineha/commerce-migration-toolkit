def notificationChannel = '#cloud-move-cmt-dev'
def color = 'good'
pipeline {
  agent {
    kubernetes {
      yamlFile '.jenkins/podtemplate.yaml'
      defaultContainer 'jdk'
    }
  }
  options {
    buildDiscarder logRotator(daysToKeepStr: '30', numToKeepStr: '5')
    ansiColor('xterm')
    timestamps()
    checkoutToSubdirectory 'commerce-migration-toolkit'
  }
  triggers {
    // add cron trigger only to 'master' branch
    cron(env.BRANCH_NAME == 'master' ? 'TZ=UTC\nH 0 * * *' : '')
  }
  environment {
    SAMPLE_PROJECT = 'cmt-sample-project'
    REPOSITORY_CREDS = credentials('common.repositories.cloud.sap')
    ORG_GRADLE_PROJECT_repositoryUser = "$REPOSITORY_CREDS_USR"
    ORG_GRADLE_PROJECT_repositoryPass = "$REPOSITORY_CREDS_PSW"
    PATH = "$WORKSPACE:$PATH"

    CI="true"
    CI_SKIP_DROP="true"
    MSSQL_HOST="localhost:1433"
    MSSQL_USR="sa"
    MSSQL_PSW="localSAPassw0rd"
    MYSQL_HOST="localhost:3306"
    MYSQL_USR="root"
    MYSQL_PSW="root"
    ORACLE_HOST="localhost:1521"
    ORACLE_USR="system"
    ORACLE_PSW="oracle"
    HANA_HOST="localhost:39017"
    HANA_USR="SYSTEM"
    HANA_PSW="HXEHana1"
  }
  stages {
    stage('Bootstrap Tools') {
      steps {
        sh '''
          wget -q --header 'Cookie: eula_3_1_agreed=tools.hana.ondemand.com/developer-license-3_1.txt' \
            https://tools.hana.ondemand.com/additional/sapjvm-8.1.075-linux-x64.zip
          jar -xf sapjvm-8*.zip
          chmod -R a+rwx,o-w ./sapjvm_8
          '''
        dir("$SAMPLE_PROJECT") {
          git url: 'https://github.tools.sap/cloud-move-programs/cmt-sample-project.git',
            changelog: false, poll: false,
            credentialsId: 'cloud-move-programs-serviceuser',
            branch: 'master'
        }
      }
    }
    stage("Startup Logs") {
      steps {
        containerLog 'mssql'
        containerLog 'mysql'
        containerLog 'oracle'
        containerLog 'hana'
      }
    }
    stage('Build and Test') {
      failFast true
      matrix {
        axes {
          axis {
            name 'COMMERCE'
            values '1811', '1905'
          }
        }
        environment {
          JAVA_HOME = "${COMMERCE.toInteger() < 1905 ? "${WORKSPACE}/sapjvm_8" : '/usr/lib/jvm/sapmachine-11'}"
        }
        stages {
          stage('Bootstrap') {
            steps {
              sh 'env'
              sh 'mkdir "$COMMERCE"'
              sh 'cd "$SAMPLE_PROJECT"; tar -c --exclude=.git . | tar -x -C "../$COMMERCE"'
              sh 'cd commerce-migration-toolkit; tar -c --exclude=.git . | tar -x -C "../$COMMERCE/core-customize/hybris/bin/custom"'
              dir("$COMMERCE") {
                sh "./setup-ci-runtime.sh $COMMERCE"
                dir('core-customize') {
                  sh 'echo "org.gradle.jvmargs=-Xmx1G -Dfile.encoding=UTF-8" > gradle.properties' 
                  sh "./switch-commerce-version.sh $COMMERCE"
                }
              }
            }
          }
          stage('Build') {
            steps {
              dir("$COMMERCE/core-customize") {
                sh './gradlew setupCIEnvironment yclean yall --stacktrace'
              }
            }
          }
          stage('Unit Test') {
            steps {
              dir("$COMMERCE/core-customize") {
                sh ' ./gradlew unitTest --stacktrace'
                sh 'mv hybris/log/junit/TESTS-TestSuites.xml "$WORKSPACE/TESTS-TestSuites.$COMMERCE.unit.xml" || true'
              }
            }
          }
          stage('Integration Test') {
            when {
              anyOf {
                changeRequest target: 'master' // pull request targeting master, OR
                changeRequest target: 'develop' // pull request targeting develop, OR
                allOf { // nightly build for master
                  branch 'master'
                  triggeredBy 'TimerTrigger'
                }
              }
            }
            stages {
              stage('Setup Integration Test Environment') {
                steps {
                  dir("$COMMERCE") {
                    dir('core-customize') {
                      sh './gradlew initialize unitinitialize --stacktrace'
                    }
                  }
                }
              }
              stage('Run Integration Tests') {
                steps {
                  dir("$COMMERCE/core-customize") {
                    sh './gradlew integrationTests --stacktrace'
                    sh 'mv hybris/log/junit/TESTS-TestSuites.xml "$WORKSPACE/TESTS-TestSuites.$COMMERCE.integration.xml" || true'
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  post {
    always {
      containerLog 'mssql'
      containerLog 'mysql'
      containerLog 'oracle'
      containerLog 'hana'
      script {
        try {
          junit allowEmptyResults: true, testResults: 'TEST*.xml'
        } catch (exc) {
          //ignore
        }
      }
      script {
        if (currentBuild.resultIsWorseOrEqualTo('ABORTED')) {
          color = '#515151'
        } else if (currentBuild.resultIsWorseOrEqualTo('FAILURE')) {
          color = 'danger'
        } else if (currentBuild.resultIsWorseOrEqualTo('UNSTABLE')) {
          color = 'warning'
        }
      }
      slackSend channel: notificationChannel,
        message: "*${currentBuild.fullDisplayName}*: *${currentBuild.result}* (<${env.RUN_DISPLAY_URL}|Details>)",
        color: color
    }
  }
}
