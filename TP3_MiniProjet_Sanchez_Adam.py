# Code du TP 3 - Mini Projet 
# Sanchez Adam

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from datetime import datetime
import uuid
import time

# Configure AWS credentials (dummy values in this case)
boto3.setup_default_session(
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKE',
    region_name='us-west-2'
)

def create_dynamodb_resource():
    """Crée une ressource DynamoDB connectée à l'instance locale."""
    return boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

def create_table(dynamodb, name):
    """Crée une table DynamoDB."""
    table_name = name
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        table.wait_until_exists()
        print(f"Table {table_name} created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table {table_name} already exists.")
        else:
            raise

def insert_item(dynamodb, table_name, item):
    """Insère un élément dans la table DynamoDB."""
    table = dynamodb.Table(table_name)
    table.put_item(Item=item)
    print(f"Item inserted: {item}")

def get_item(dynamodb, table_name, key):
    """Récupère un élément de la table DynamoDB."""
    table = dynamodb.Table(table_name)
    response = table.get_item(Key=key)
    return response.get('Item')

def check_table_exists(dynamodb, table_name):
    # Utiliser le client DynamoDB pour lister les tables
    client = dynamodb.meta.client

    # Initialisation pour la pagination
    paginator = client.get_paginator('list_tables')
    page_iterator = paginator.paginate()

    # Parcourir toutes les tables pour voir si table_name existe
    for page in page_iterator:
        if table_name in page['TableNames']:
            return True
        
    return False

def scan_all_items(dynamodb, table_name):
    # Initialisation du client DynamoDB
    table = dynamodb.Table(table_name)
    print("Scanning table...")
    
    # Scan de la table
    response = table.scan()
    
    # Récupération des éléments
    items = response['Items']
    
    # Affichage des éléments
    for item in items:
        print(item)
    
    # Gérer la pagination si la réponse est paginée
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items = response['Items']
        for item in items:
            print(item)

def drop_table(dynamodb, table_name):
    """Supprime une table si elle existe."""
    try:
        table = dynamodb.Table(table_name)
        table.delete()
        table.wait_until_not_exists()
        print(f"Table {table_name} supprimée avec succès.")
    except ClientError as e:
        print(f"Erreur lors de la suppression de {table_name}: {e}")


def update_item(dynamodb, table_name, key, updated_info):
    """Met à jour un élément de la table DynamoDB."""
    table = dynamodb.Table(table_name)
    update_expression = "SET "
    expression_attribute_values = {}
    
    # Préparer l'expression de mise à jour et les valeurs correspondantes
    for k, value in updated_info.items():
        update_expression += f"{k} = :{k}, "
        expression_attribute_values[f":{k}"] = value
    
    # Retirer la dernière virgule
    update_expression = update_expression.rstrip(', ')

    # Exécuter la mise à jour
    response = table.update_item(
        Key=key,  # Clé primaire pour la mise à jour
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues="UPDATED_NEW"
    )
    print(f"Item updated: {response['Attributes']}")



def create_tables(dynamodb):
    """Crée les tables Livres et Emprunts si elles n'existent pas."""
    tables = {
        'Livres': {
            'KeySchema': [{'AttributeName': 'ISBN', 'KeyType': 'HASH'}],
            'AttributeDefinitions': [{'AttributeName': 'ISBN', 'AttributeType': 'S'}],
        },
        'Emprunts': {
            'KeySchema': [{'AttributeName': 'emprunt_id', 'KeyType': 'HASH'}],
            'AttributeDefinitions': [{'AttributeName': 'emprunt_id', 'AttributeType': 'S'}],
        }
    }
    
    for table_name, schema in tables.items():
        if not check_table_exists(dynamodb, table_name):
            try:
                table = dynamodb.create_table(
                    TableName=table_name,
                    KeySchema=schema['KeySchema'],
                    AttributeDefinitions=schema['AttributeDefinitions'],
                    ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                )
                table.wait_until_exists()
                print(f"Table {table_name} créée avec succès.")
            except ClientError as e:
                print(f"Erreur lors de la création de {table_name}: {e}")
        else:
            print(f"Table {table_name} existe déjà.")
    
    # Vérification si les tables ont bien été créées
    for table_name in tables.keys():
        if not check_table_exists(dynamodb, table_name):
            print(f"Erreur : La table {table_name} n'a pas été créée.")
        else:
            print(f"La table {table_name} existe bien.")


def ajouter_livre(dynamodb, livre):
    """Ajoute un nouveau livre dans la table Livres."""
    table = dynamodb.Table('Livres')
    table.put_item(Item=livre)
    print(f"Livre ajouté : {livre}")


def get_livre(dynamodb, isbn=None):
    """Récupère un livre par son ISBN ou liste tous les livres."""
    table = dynamodb.Table('Livres')
    if isbn:
        response = table.get_item(Key={'ISBN': isbn})
        return response.get('Item')
    else:
        response = table.scan()
        return response.get('Items', [])


def update_livre(dynamodb, isbn, updated_info):
    """Met à jour les informations d'un livre."""
    table = dynamodb.Table('Livres')
    update_expression = "SET "
    expression_attribute_values = {}
    
    # Préparation de l'expression de mise à jour et des valeurs
    for key, value in updated_info.items():
        update_expression += f"{key} = :{key}, "
        expression_attribute_values[f":{key}"] = value
    
    # Supprimer la dernière virgule
    update_expression = update_expression.rstrip(', ')
    
    # Exécuter la mise à jour
    response = table.update_item(
        Key={'ISBN': isbn},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues="UPDATED_NEW"
    )
    print(f"Livre mis à jour : {response['Attributes']}")


def delete_livre(dynamodb, isbn):
    """Supprime un livre de la table Livres."""
    table = dynamodb.Table('Livres')
    table.delete_item(Key={'ISBN': isbn})
    print(f"Livre avec ISBN {isbn} supprimé.")


def emprunter_livre(dynamodb, isbn, utilisateur):
    """Permet à un utilisateur d'emprunter un livre."""
    table_livres = dynamodb.Table('Livres')
    table_emprunts = dynamodb.Table('Emprunts')

    # Récupérer le livre par son ISBN
    livre = get_item(dynamodb, 'Livres', {'ISBN': isbn})
    
    if livre and livre['disponible']:
        # Ajouter un emprunt dans la table Emprunts
        emprunt_id = str(uuid.uuid4())  # Générer un ID unique pour l'emprunt
        date_emprunt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item_emprunt = {
            'emprunt_id': emprunt_id,
            'ISBN': isbn,
            'utilisateur': utilisateur,
            'date_emprunt': date_emprunt,
            'date_retour': None
        }
        insert_item(dynamodb, 'Emprunts', item_emprunt)

        # Mettre à jour la disponibilité du livre
        livre['disponible'] = False
        update_livre(dynamodb, isbn, {'disponible': False})

        print(f"Le livre avec ISBN {isbn} a été emprunté par {utilisateur}.")
    else:
        print(f"Le livre avec ISBN {isbn} n'est pas disponible ou n'existe pas.")


def retourner_livre(dynamodb, isbn, utilisateur):
    """Permet à un utilisateur de retourner un livre."""
    table_emprunts = dynamodb.Table('Emprunts')

    # Récupérer l'emprunt par l'ISBN du livre
    response = table_emprunts.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr('ISBN').eq(isbn) & 
                         boto3.dynamodb.conditions.Attr('utilisateur').eq(utilisateur) & 
                         boto3.dynamodb.conditions.Attr('date_retour').eq(None)
    )
    
    emprunts = response.get('Items', [])
    
    if len(emprunts) > 0:
        # Prendre le premier emprunt trouvé
        emprunt = emprunts[0]
        emprunt_id = emprunt['emprunt_id']
        
        # Mettre à jour la date de retour de l'emprunt
        date_retour = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_item(dynamodb, 'Emprunts', {'emprunt_id': emprunt_id}, {'date_retour': date_retour})

        # Mettre à jour la disponibilité du livre
        update_livre(dynamodb, isbn, {'disponible': True})

        print(f"Le livre avec ISBN {isbn} a été retourné.")
    else:
        print(f"Le livre avec ISBN {isbn} n'a pas été emprunté.")


def trouver_livres_auteur(dynamodb, auteur):
    """Trouve tous les livres d'un auteur donné."""
    table = dynamodb.Table('Livres')
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr('auteur').eq(auteur)
    )
    return response.get('Items', [])


def lister_emprunts_qui_depassent_duree(dynamodb, duree):
    """Liste les emprunts qui dépassent une durée donnée en faisant la difference entre sa date de retour et sa date d'emprunt."""
    table = dynamodb.Table('Emprunts')
    response = table.scan()
    emprunts = response.get('Items', [])
    emprunts_depasses = []
    
    for emprunt in emprunts:
        if emprunt['date_retour']:
            date_emprunt = datetime.strptime(emprunt['date_emprunt'], "%Y-%m-%d %H:%M:%S")
            date_retour = datetime.strptime(emprunt['date_retour'], "%Y-%m-%d %H:%M:%S")
            duree_emprunt = date_retour - date_emprunt
            if duree_emprunt.days > duree:
                emprunts_depasses.append(emprunt)
    
    return emprunts_depasses
    

def livres_les_plus_empruntes(dynamodb):
    """Trouve les livres les plus empruntés et renvoie seulement le top 5"""
    table_emprunts = dynamodb.Table('Emprunts')
    response = table_emprunts.scan()
    emprunts = response.get('Items', [])
    
    # Compter le nombre d'emprunts pour chaque livre
    livres_empruntes = {}
    for emprunt in emprunts:
        isbn = emprunt['ISBN']
        if isbn in livres_empruntes:
            livres_empruntes[isbn] += 1
        else:
            livres_empruntes[isbn] = 1
    
    # Trier les livres par nombre d'emprunts
    livres_empruntes = {k: v for k, v in sorted(livres_empruntes.items(), key=lambda item: item[1], reverse=True)}
    
    # Récupérer les 5 livres les plus empruntés
    top_5_livres = list(livres_empruntes.keys())[:5]
    
    # Récupérer les informations de ces livres
    livres = []
    for isbn in top_5_livres:
        livre = get_item(dynamodb, 'Livres', {'ISBN': isbn})
        livres.append(livre)
    
    return livres
    


def main():
    """Point d'entrée du script."""
    dynamodb = create_dynamodb_resource()

    drop_table(dynamodb, 'Livres')
    drop_table(dynamodb, 'Emprunts')

    # On créer nos tables
    create_tables(dynamodb)
    
    # Affichage du menu principal
    while True:
        print("\n--- Menu ---")
        print("1. Ajouter un livre")
        print("2. Consulter les livres")
        print("3. Emprunter un livre")
        print("4. Retourner un livre")
        print("5. Consulter les emprunts")
        print("6. Liste des livres d'un auteur")
        print("7. Liste des emprunts qui dépasse une durée")
        print("8. Liste des livres les plus empruntés")
        print("9. Quitter")
        choix = input("Veuillez entrer votre choix (1-9) : ")
        
        if choix == "1":
            # Ajouter un livre
            print("\n--- Ajouter un livre ---")
            isbn = input("ISBN du livre : ")
            titre = input("Titre du livre : ")
            auteur = input("Auteur du livre : ")
            annee_publication = input("Année de publication : ")
            disponible = input("Le livre est-il disponible ? (oui/non) : ").lower() == "oui"
            
            livre = {
                'ISBN': isbn,
                'titre': titre,
                'auteur': auteur,
                'année_publication': int(annee_publication),
                'disponible': disponible
            }
            
            ajouter_livre(dynamodb, livre)
        
        elif choix == "2":
            # Consulter les livres
            print("\n--- Liste des livres ---")
            response = dynamodb.Table('Livres').scan()
            livres = response.get('Items', [])
            if livres:
                for livre in livres:
                    print(f"ISBN: {livre['ISBN']}, Titre: {livre['titre']}, Auteur: {livre['auteur']}, Disponible: {livre['disponible']}")
            else:
                print("Il n'y a aucun livres dans la table.")
        
        elif choix == "3":
            # Emprunter un livre
            print("\n--- Emprunter un livre ---")
            isbn = input("ISBN du livre à emprunter : ")
            utilisateur = input("Nom de l'utilisateur : ")
            
            emprunter_livre(dynamodb, isbn, utilisateur)
        
        elif choix == "4":
            # Retourner un livre
            print("\n--- Retourner un livre ---")
            isbn = input("ISBN du livre à retourner : ")
            utilisateur = input("Nom de l'utilisateur : ")
            
            retourner_livre(dynamodb, isbn, utilisateur)
        
        elif choix == "5":
            # Consulter les emprunts
            print("\n--- Liste des emprunts ---")
            response = dynamodb.Table('Emprunts').scan()
            emprunts = response.get('Items', [])
            if emprunts:
                for emprunt in emprunts:
                    print(f"Emprunt ID: {emprunt['emprunt_id']}, ISBN: {emprunt['ISBN']}, Utilisateur: {emprunt['utilisateur']}, Date d'emprunt: {emprunt['date_emprunt']}, Date de retour: {emprunt['date_retour']}")
            else:
                print("Il n'y a aucun emprunts dans la table.")
        
        elif choix == "6":
            # Liste des livres d'un auteur
            print("\n--- Liste des livres d'un auteur ---")
            auteur = input("Nom de l'auteur : ")
            livres = trouver_livres_auteur(dynamodb, auteur)
            if livres:
                for livre in livres:
                    print(f"ISBN: {livre['ISBN']}, Titre: {livre['titre']}, Auteur: {livre['auteur']}, Disponible: {livre['disponible']}")
            else:
                print(f"Il n'y a aucun livre de {auteur}.")

        elif choix == "7":
            # Liste des emprunts qui dépassent une durée
            print("\n--- Liste des emprunts qui dépassent une durée ---")
            duree = int(input("Durée en jours (-1 pour tout avoir): "))
            emprunts_depasses = lister_emprunts_qui_depassent_duree(dynamodb, duree)
            if emprunts_depasses:
                for emprunt in emprunts_depasses:
                    print(f"Emprunt ID: {emprunt['emprunt_id']}, ISBN: {emprunt['ISBN']}, Utilisateur: {emprunt['utilisateur']}, Date d'emprunt: {emprunt['date_emprunt']}, Date de retour: {emprunt['date_retour']}")
            else:
                print(f"Il n'y a aucun emprunts qui dépassent {duree} jours.")

        elif choix == "8":
            # Liste des livres les plus empruntés
            print("\n--- Liste des livres les plus empruntés ---")
            livres = livres_les_plus_empruntes(dynamodb)
            if livres:
                for livre in livres:
                    print(f"ISBN: {livre['ISBN']}, Titre: {livre['titre']}, Auteur: {livre['auteur']}, Disponible: {livre['disponible']}")
            else:
                print("Il n'y a aucun livre dans la table.")

        elif choix == "9":
            print("Arrêt du programme.")
            break
        
        else:
            print("Choix invalide, veuillez réessayer.")



if __name__ == '__main__':
    main()
