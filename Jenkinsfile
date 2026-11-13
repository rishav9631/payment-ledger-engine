pipeline {
    agent any

    environment {
        JAVA_HOME = tool 'JDK21'
        MAVEN_OPTS = '-Xmx512m -XX:+UseG1GC'
        IMAGE_NAME = 'payment-ledger-engine'
    }

    stages {
        stage('Checkout') {
            steps {
                echo 'Checking out source repository...'
                checkout scm
            }
        }

        stage('Compile & Static Verification') {
            steps {
                echo 'Compiling Java 21 sources...'
                sh 'mvn clean compile -DskipTests'
            }
        }

        stage('Unit & Concurrency Tests') {
            steps {
                echo 'Running unit and validation test suites...'
                sh 'mvn test'
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'target/surefire-reports/*.xml'
                }
            }
        }

        stage('Package Application') {
            steps {
                echo 'Building executable Spring Boot jar...'
                sh 'mvn package -DskipTests'
            }
        }

        stage('Docker Image Build') {
            steps {
                echo 'Building Docker container...'
                sh "docker build -t ${IMAGE_NAME}:${BUILD_NUMBER} -t ${IMAGE_NAME}:latest ."
            }
        }
    }

    post {
        always {
            cleanWs deleteDirs: true, notFailBuild: true
        }
        success {
            echo "CI pipeline completed successfully for ${IMAGE_NAME} Build #${BUILD_NUMBER}!"
        }
        failure {
            echo "CI pipeline failed for ${IMAGE_NAME} Build #${BUILD_NUMBER}."
        }
    }
}

