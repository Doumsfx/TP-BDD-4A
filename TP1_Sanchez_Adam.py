# Code du TP 1
# Sanchez Adam

import json
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pymongo

# Étape 1: Connexion à la base de données MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')

db = client['TP_01'] # Nom de la base de données
collection = db['SuperHeroes'] # Nom de la collection

"""
# Étape 2: Importer les données des super-héros dans MongoDB
# 1. Préparer les données JSON
with open('SuperHerosComplet.json', 'r') as file:
 data = json.load(file)

# 2. Insérer les données dans MongoDB
result = collection.insert_many(data)
print('Inserted {} documents'.format(len(result.inserted_ids)))

# 3. Vérifier les insertions
print(collection.find_one())

"""


def etape4():
    # On créer les indexs
    collection.create_index([("name",pymongo.ASCENDING)])
    collection.create_index([("powerstats.intelligence", pymongo.ASCENDING)])
    collection.create_index([("biography.publisher", pymongo.ASCENDING)])

def etape5():
    # On extrait les données tout en convertissant le résultat en liste de dictionnaire
    documents = list(collection.find({}))

    # On charge les données dans un DataFrame
    df = pd.DataFrame(documents)

    print(df)

# On extrait les données tout en convertissant le résultat en liste de dictionnaire
documents = list(collection.find({}))

# On charge les données dans un DataFrame
df = pd.DataFrame(documents)

#print(df)

# Met 'Unknown' pour les valeurs inconnu
def clean_nested_data(value):
    if isinstance(value, dict):
        return {key: clean_nested_data(val) for key, val in value.items()}
    elif isinstance(value, list):
        return [clean_nested_data(val) for val in value]
    elif value in [None, "-", np.nan]:
        return "Unknown"
    else:
        return value
    
# Conversion pour height (en cm) : si la valeur est en "ft" ou "in", on la convertit en cm
def convert_height(height):
    if isinstance(height, list):
        # Cherche la valeur en cm
        cm_value = next((x for x in height if "cm" in x), None)
        if cm_value:
            return float(cm_value.replace(" cm", ""))
        else:
            # Si la valeur en cm n'existe pas, effectue la conversion de pieds/pouces en cm
            ft_value = next((x for x in height if "ft" in x), None)
            if ft_value:
                feet, inches = ft_value.replace("'", " ").split()
                total_in_inches = (float(feet) * 12) + float(inches)
                return total_in_inches * 2.54
            return 0
    return 0

# Conversion pour weight (en kg) : si la valeur est en "lb", on la convertit en kg
def convert_weight(weight):
    if isinstance(weight, list):
        # Cherche la valeur en kg
        kg_value = next((x for x in weight if "kg" in x), None)
        if kg_value:
            return float(kg_value.replace(" kg", ""))
        else:
            # Si la valeur en kg n'existe pas, effectue la conversion de lb en kg
            lb_value = next((x for x in weight if "lb" in x), None)
            if lb_value:
                return float(lb_value.replace(" lb", "")) * 0.453592
            return 0
    return 0

def etape6(df):
    # On remplace toutes caractéristiques incoonues par 'Unknown'
    df = df.map(clean_nested_data)
    
    # On applique la conversion sur la colonne 'appearance' en utilisant une fonction lambda
    df["appearance"] = df["appearance"].apply(
        lambda x: {
            "height_cm": convert_height(x["height"]),
            "weight_kg": convert_weight(x["weight"]),
            **x  # Conserve le reste des informations sans modification
        }
    )

    # On supprime les colonnes 'height' et 'weight' après les avoir converties
    df["appearance"] = df["appearance"].apply(
        lambda x: {k: v for k, v in x.items() if k not in ["height", "weight"]}  # Supprime height et weight
    )

    # On affiche quelques tests
    # print(f"Héro 7 (appearance) : {df.iloc[6]['appearance']}")
    # print(f"Héro 8 (appearance) : {df.iloc[7]['appearance']}")

    return df

# Fonction pour afficher le menu et gérer les choix
def etape7(df, db):
    choix = 0
    while(choix != 7):
        print("Menu:")
        print("1. Statistiques (moyenne, médiane, variance) (Question 8)")
        print("2. Statistiques (moyenne, médiane, variance) (Question 9)")
        print("3. Histogrammes pour visualiser la distribution de l'intelligence et de la force des super-héros (Question 10)")
        print("4. Graphiques à barres pour comparer le nombre de super-héros par éditeur (Question 11)")
        print("5. Création de vues (Question 12)")
        print("6. Visualisation et analyse de celle-ci (Question 13)")
        print("7. Quitter")
        
        choix = input("Choisissez une action (1-7): ")
        
        if choix == '1':
            etape8(df)
        elif choix == '2':
            etape9(df)
        elif choix == '3':
            etape10(df)
        elif choix == '4':
            etape11(df)
        elif choix == '5':
            etape12(df)
        elif choix == '6':
            etape13(db)
        elif choix == '7':
            break
        else:
            print("Choix invalide, veuillez réessayer.")

def etape8(df):   
    # Charger les données depuis MongoDB dans un DataFrame
    df = pd.DataFrame(list(collection.find()))
    
    # Extraire les powerstats (force, intelligence, vitesse) dans une nouvelle DataFrame
    powerstats = df['powerstats'].apply(pd.Series)

    # Calculer les statistiques descriptives
    print("\nStatistiques descriptives des super-héros :")
    print(f"\nMoyenne de la force : {powerstats['strength'].mean():.2f}")
    print(f"Moyenne de l'intelligence : {powerstats['intelligence'].mean():.2f}")
    print(f"Moyenne de la vitesse : {powerstats['speed'].mean():.2f}")
    
    print(f"\nMédiane de la force : {powerstats['strength'].median():.2f}")
    print(f"Médiane de l'intelligence : {powerstats['intelligence'].median():.2f}")
    print(f"Médiane de la vitesse : {powerstats['speed'].median():.2f}")
    
    print(f"\nVariance de la force : {powerstats['strength'].var():.2f}")
    print(f"Variance de l'intelligence : {powerstats['intelligence'].var():.2f}")
    print(f"Variance de la vitesse : {powerstats['speed'].var():.2f}")

def etape9(df):
    # Si df est un DataFrame Pandas, on le convertit en une liste de dictionnaires
    if isinstance(df, pd.DataFrame):
        df = df.to_dict(orient='records')
    
    # Extraire les valeurs des powerstats (force, intelligence, vitesse) et les convertir en array NumPy
    strength = np.array([hero['powerstats'].get('strength', 0) for hero in df])
    intelligence = np.array([hero['powerstats'].get('intelligence', 0) for hero in df])
    speed = np.array([hero['powerstats'].get('speed', 0) for hero in df])

    # Calcul des statistiques descriptives avec NumPy
    print("\nStatistiques descriptives des super-héros :")
    
    # Moyenne
    print(f"\nMoyenne de la force : {np.mean(strength):.2f}")
    print(f"Moyenne de l'intelligence : {np.mean(intelligence):.2f}")
    print(f"Moyenne de la vitesse : {np.mean(speed):.2f}")
    
    # Médiane
    print(f"\nMédiane de la force : {np.median(strength):.2f}")
    print(f"Médiane de l'intelligence : {np.median(intelligence):.2f}")
    print(f"Médiane de la vitesse : {np.median(speed):.2f}")
    
    # Variance
    print(f"\nVariance de la force : {np.var(strength):.2f}")
    print(f"Variance de l'intelligence : {np.var(intelligence):.2f}")
    print(f"Variance de la vitesse : {np.var(speed):.2f}")

def etape10(df):
    # Si df est un DataFrame Pandas, on le convertit en une liste de dictionnaires
    if isinstance(df, pd.DataFrame):
        df = df.to_dict(orient='records')
    
    # Extraire les valeurs de force et d'intelligence
    strength = np.array([hero['powerstats'].get('strength', 0) for hero in df])
    intelligence = np.array([hero['powerstats'].get('intelligence', 0) for hero in df])
    
    # Créer un histogramme pour la distribution de la force
    plt.figure(figsize=(10, 5))
    plt.subplot(1, 2, 1)
    plt.hist(strength, bins=20, color='blue', alpha=0.7)
    plt.title('Distribution de la Force des Super-héros')
    plt.xlabel('Force')
    plt.ylabel('Nombre de Super-héros')

    # Créer un histogramme pour la distribution de l'intelligence
    plt.subplot(1, 2, 2)
    plt.hist(intelligence, bins=20, color='green', alpha=0.7)   
    plt.title('Distribution de l\'Intelligence des Super-héros')
    plt.xlabel('Intelligence')
    plt.ylabel('Nombre de Super-héros')

    # Afficher les graphiques
    plt.tight_layout()
    plt.show()

def etape11(df):
    # Si df est un DataFrame Pandas, on le convertit en une liste de dictionnaires
    if isinstance(df, pd.DataFrame):
        df = df.to_dict(orient='records')

    # Extraire les éditeurs
    publishers = [hero['biography'].get('publisher', 'Unknown') for hero in df]

    # Compter le nombre de super-héros par éditeur
    publisher_counts = pd.Series(publishers).value_counts()

    # Créer un graphique à barres
    plt.figure(figsize=(10, 6))
    publisher_counts.plot(kind='bar', color='skyblue')
    plt.title('Nombre de Super-héros par Éditeur')
    plt.xlabel('Éditeur')
    plt.ylabel('Nombre de Super-héros')
    plt.tight_layout() 
    plt.show()
    
def etape12(df):
    # Si df est un DataFrame Pandas, on le convertit en une liste de dictionnaires
    if isinstance(df, pd.DataFrame):
        df = df.to_dict(orient='records')

    # Utilisation de l'agrégation MongoDB pour calculer la moyenne de l'intelligence
    pipeline = [
        {
            "$group": {
                "_id": None,
                "moyenne_intelligence": {"$avg": "$powerstats.intelligence"}
            }
        }
    ]

    result = list(db.SuperHeroes.aggregate(pipeline))
    average_intelligence = result[0]["moyenne_intelligence"]


    print(f"Intelligence moyenne : {average_intelligence}")

    # Créer une vue MongoDB pour les super-héros avec une intelligence supérieure à la moyenne
    db.command('create', 'superheros_intelligents', viewOn="SuperHeroes", pipeline=[
        {"$match": {"powerstats.intelligence": {"$gt": average_intelligence}}}
    ])
    print("Vue créée pour les super-héros avec intelligence supérieure à la moyenne.")

    # Créer une vue MongoDB pour les super-héros classés par force (force décroissante)
    db.command('create', 'superheros_par_force', viewOn="SuperHeroes", pipeline=[
        {"$sort": {"powerstats.strength": -1}}  # Tri décroissant par force
    ])
    print("Vue créée pour les super-héros classés par force.")

def etape13(db):
    # Récupérer les données des vues MongoDB
    superheros_intelligents = list(db.superheros_intelligents.find())
    superheros_par_force = list(db.superheros_par_force.find())

    # Extraire les valeurs nécessaires pour les graphiques
    intelligence = [hero['powerstats']['intelligence'] for hero in superheros_intelligents]
    force = [hero['powerstats']['strength'] for hero in superheros_par_force]

    # Visualisation 1 : Histogramme de l'intelligence des super-héros avec intelligence > moyenne
    plt.figure(figsize=(10, 5))
    plt.hist(intelligence, bins=20, color='blue', edgecolor='black', alpha=0.7)
    plt.title("Distribution de l'intelligence des super-héros avec intelligence > moyenne")
    plt.xlabel("Intelligence")
    plt.ylabel("Nombre de super-héros")
    plt.grid(True)
    plt.show()

    # Visualisation 2 : Histogramme de la force des super-héros classés par force (force décroissante)
    plt.figure(figsize=(10, 5))
    plt.barh(range(len(force)), force, color='green')
    plt.title("Classement des super-héros par force")
    plt.xlabel("Force")
    plt.ylabel("Super-héros (Index trié)")
    plt.grid(True)
    plt.show()


df = etape6(df)
etape7(df, db)

