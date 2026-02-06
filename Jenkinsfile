// CI: build, test, Docker image build, registry push. Run as batch/cron in CD.
pipeline {
    agent any
    stages {
        stage('Docker Build') {
            steps {
                script {
                    def tag = env.GIT_COMMIT?.take(7) ?: 'latest'
                    sh "docker build -t investment-data-collector:${tag} ."
                }
            }
        }
    }
}
