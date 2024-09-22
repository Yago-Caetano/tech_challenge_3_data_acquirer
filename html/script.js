document.getElementById('airQualityForm').addEventListener('submit', function(e) {
    e.preventDefault();

    const pm25 = document.getElementById('pm25').value;
    const pm10 = document.getElementById('pm10').value;

    // URL da API Gateway
    const apiUrl = `https://k9kmqgyax9.execute-api.us-east-1.amazonaws.com/default/execute_kmeans_lambda?x=${pm25}&y=${pm10}`

    // Fazendo a requisição GET
    fetch(apiUrl)
        .then(response => {
            console.log(response)
            if (!response.ok) {
                throw new Error('Erro ao obter a resposta da API.');
            }
            return response.json();
        })
        .then(data => {
            // Exibir o resultado na interface
            document.getElementById('result').innerHTML = `Grupo de localidades: ${data.prediction}`;
        })
        .catch(error => {
            console.error('Erro:', error);
            document.getElementById('result').innerHTML = 'Ocorreu um erro ao processar sua requisição.';
        });
});
