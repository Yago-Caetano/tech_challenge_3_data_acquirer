from flask import Flask, request, jsonify
import joblib
import numpy as np

# Inicializando a aplicação Flask
app = Flask(__name__)

# Carregando o modelo KMeans salvo do disco
model_path = 'kmeans_model.pkl'  
kmeans = joblib.load(model_path)

# Rota para consultar o modelo com novos dados
@app.route('/predict', methods=['POST'])
def predict():
    try:
        data = request.json['data'] 
        data = np.array(data).reshape(1, -1)    
        
        # Fazendo a previsão com o modelo KMeans
        cluster = kmeans.predict(data)
        
        # Retornando o resultado em formato JSON
        return jsonify({'cluster': int(cluster[0])})
    
    except Exception as e:
        # Em caso de erro, retornamos uma mensagem de erro
        return jsonify({'error': str(e)}), 400

# Rodando a aplicação
if __name__ == '__main__':
    app.run(debug=True)
