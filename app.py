# Importer des modules nécessaires
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
from bson import json_util, ObjectId
import json
from datetime import datetime
from pymongo import DESCENDING
from dotenv import load_dotenv
import os

# Charger les variables d'environnement à partir du fichier .env
load_dotenv()

# Configuration de MongoDB en utilisant les variables d'environnement
mongodb_url = os.getenv("MONGODB_URL")  # URL de la base de données MongoDB
client = MongoClient(mongodb_url)
db = client['bigdata']  # Sélectionner la base de données
gouv_collection = db['gouv_data']  # Sélectionner la collection

# Initialiser l'application Flask
app = Flask(__name__)
CORS(app)

# Route pour obtenir toutes les données
@app.route('/gouv_data', methods=['GET'])
def get_all_data():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    skip = (page - 1) * per_page

    # Triez les données par date "inserted_at" la plus récente (ordre décroissant)
    gouv_data_cursor = gouv_collection.find().sort("inserted_at", DESCENDING).skip(skip).limit(per_page)

    gouv_data_list = list(gouv_data_cursor)

    if not gouv_data_list:
        return jsonify({"error": "No data found"}), 404

    gouv_data_list_sanitized = json.loads(json_util.dumps(gouv_data_list))
    return jsonify(gouv_data_list_sanitized), 200

# Route pour créer de nouvelles données
@app.route('/gouv_data', methods=['POST'])
def create_data():
    new_data = request.json
    new_data['inserted_at'] = datetime.utcnow()  # Ajouter la date et l'heure actuelles
    result = gouv_collection.insert_one(new_data)
    # Renvoyer l'ID créé en tant que chaîne
    created_id = str(result.inserted_id)
    return jsonify({"message": "Created successfully", "id": created_id}), 201

# Route pour mettre à jour des données existantes
@app.route('/gouv_data/<id>', methods=['PUT'])
def update_data(id):
    updated_data = request.json
    existing_data = gouv_collection.find_one({"_id": ObjectId(id)})

    if existing_data is None:
        return jsonify({"error": "No data found with the given ID"}), 404

    # Assurez-vous que les champs "fields" existent dans les données existantes
    if "fields" not in existing_data:
        existing_data["fields"] = {}

    # Mettez à jour les champs "1_f_commune_pdl" et "date_des_donnees" si présents dans les données mises à jour
    if "fields" in updated_data:
        if "1_f_commune_pdl" in updated_data["fields"]:
            existing_data["fields"]["1_f_commune_pdl"] = updated_data["fields"]["1_f_commune_pdl"]
        if "date_des_donnees" in updated_data["fields"]:
            existing_data["fields"]["date_des_donnees"] = updated_data["fields"]["date_des_donnees"]
    
    # Mettez également à jour les champs "datasetid" et "recordid" si présents dans les données mises à jour
    if "datasetid" in updated_data:
        existing_data["datasetid"] = updated_data["datasetid"]
    if "recordid" in updated_data:
        existing_data["recordid"] = updated_data["recordid"]

    result = gouv_collection.update_one({"_id": ObjectId(id)}, {"$set": existing_data})

    if result.modified_count == 0:
        return jsonify({"error": "No data found with the given ID"}), 404

    # Renvoyer l'ID mis à jour en tant que chaîne et les données mises à jour
    updated_id = str(existing_data['_id'])
    updated_data['inserted_at'] = existing_data['inserted_at']
    return jsonify({"message": "Updated successfully", "id": updated_id, "data": updated_data}), 200

# Route pour supprimer des données
@app.route('/gouv_data/<id>', methods=['DELETE'])
def delete_data(id):
    gouv_collection.delete_one({"_id": ObjectId(id)})
    return jsonify({"message": "Deleted successfully"}), 200

# Point d'entrée de l'application Flask
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
