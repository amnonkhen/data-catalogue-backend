import json
import os
from functools import wraps

from bson import json_util
from bson.objectid import ObjectId
from flask import Flask, request, redirect, url_for, session, jsonify
from flask_cors import CORS
from flask_restful import Api, Resource
from jsonschema import validate, ValidationError
from pymongo import MongoClient
import logging

app = Flask(__name__)

# Configure secret key for session management
app.config['SECRET_KEY'] = 'your_secret_key'

CORS(app)  # Enable CORS for all routes

api = Api(app)

# MongoDB setup
mongo_host = os.getenv("MONGO_HOST", "localhost")
mongo_port = int(os.getenv("MONGO_PORT", "27017"))
mongo_uri = f"mongodb://{mongo_host}:{mongo_port}/"
mongo_db_name = os.getenv("MONGO_DB_NAME", "documents")
mongo_collection_name = os.getenv("MONGO_COLLECTION_NAME", "documents")

client = MongoClient(mongo_uri)
db = client[mongo_db_name]
collection = db[mongo_collection_name]

# Load JSON Schema from environment variable
schema_file = os.getenv("SCHEMA_FILE", "schema.json")


with open(schema_file, "r") as f:
    schema = json.load(f)


class Document(Resource):

    def get(self, submission_id=None):
        if submission_id:
            submission = collection.find_one({"_id": ObjectId(submission_id)})
            if submission:
                return self.halify(submission)
            else:
                return {"error": "document not found"}, 404
        else:
            submissions = list(collection.find())
            serialized_submissions = [json_util.loads(json_util.dumps(submission)) for submission in submissions]
            return {
                "_embedded": {"documents": list(map(self.halify, serialized_submissions))},
                "_links": {"self": {"href": url_for("document", _external=True)}}
            }

    def post(self):
        try:
            data = request.json

            # Validate incoming JSON against schema
            validate(instance=data, schema=schema)

            # Insert validated document into MongoDB
            submission_id = collection.insert_one(data).inserted_id

            return {"message": "Document submitted successfully",
                    "document_id": str(submission_id),
                    "_links": {
                        "document": {
                            "href": url_for('document', submission_id=str(submission_id), _external=True)
                        }
                    }
                    }, 201

        except ValidationError as e:
            logging.error("Validation error: %s", e)
            return {"error": str(e)}, 400
        except Exception as e:
            logging.error("Unexpected error: %s", e)
            return {"error": str(e)}, 500

    def put(self, submission_id):
        try:
            data = request.json

            # Validate incoming JSON against schema
            validate(instance=data, schema=schema)

            # Exclude the _id field from the update data
            if '_id' in data:
                del data['_id']

            # Update document in MongoDB
            result = collection.update_one({"_id": ObjectId(submission_id)}, {"$set": data})
            if result.modified_count:
                return {"message": "Document updated successfully"}, 200
            else:
                return {"error": "No document found to update"}, 404

        except ValidationError as e:
            logging.error("Validation error: %s", e)
            return {"error": str(e)}, 400
        except Exception as e:
            logging.error("Unexpected error: %s", e)
            return {"error": str(e)}, 500

    def delete(self, submission_id):
        result = collection.delete_one({"_id": ObjectId(submission_id)})
        if result.deleted_count:
            return {"message": "Document deleted successfully"}, 200
        else:
            return {"error": "No document found to delete"}, 404

    def halify(self, submission):
        submission['_id'] = str(submission['_id'])  # Convert ObjectId to string
        submission['_links'] = {
            'self': {
                'href': url_for('document', submission_id=str(submission['_id']), _external=True)
            },
            "schema": {
                "href": url_for('schema', _external=True)
            }
        }
        return submission

    class Schema(Resource):
        def get(self):
            return schema  # Return the schema document


class Root(Resource):
    def get(self):
        return {
            "_links": {
                "self": {"href": url_for("root", _external=True)},
                "document": {"href": url_for("document", _external=True)}
            }
        }


api.add_resource(Document, '/document', '/document/<string:submission_id>')
api.add_resource(Document.Schema, '/document/schema')
api.add_resource(Root, '/')

if __name__ == '__main__':
    app.run(debug=True)
