# TP Pratique : Traitement en Temps Réel avec Kafka Streams

## Objectif du TP
Ce travail pratique vise à illustrer les concepts de traitement de données en temps réel en utilisant Apache Kafka Streams à travers trois exemples concrets :

1. Conversion en Majuscules
2. Analyse de Données Météorologiques
3. Calcul Total des Commandes Clients

## Contexte Pédagogique
L'objectif est de comprendre comment Kafka Streams permet de :
- Transformer des flux de données en temps réel
- Effectuer des opérations de mappage, filtrage et agrégation
- Traiter des données de manière distribuée et scalable


## Exemple 1 : ConvertirMajuscule.java
### Objectif
Démontrer la transformation simple de flux de texte en majuscules

### Fonctionnalités
- Lecture depuis un topic source
- Conversion de tous les messages en majuscules
- Écriture dans un topic de destination

## Exemple 2 : DonneesMeteorologiques.java
### Objectif
Traiter et analyser des données météorologiques en temps réel

### Fonctionnalités
- Filtrage des relevés météorologiques
- Calcul des moyennes
- Transformation des données
- Agrégation par station météorologique

## Exemple 3 : TotalCommandeClient.java
### Objectif
Calculer le total des commandes par client en temps réel

### Fonctionnalités
- Agrégation des montants de commandes
- Calcul du total par client
- Suivi en temps réel des dépenses

## Configuration Kafka

### Création des Topics
```bash
# Créer les topics nécessaires
kafka-console-producer --topic weather-data --bootstrap-server localhost:9092
kafka-console-consumer --topic station-averages--bootstrap-server localhost:9092 --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property 

```


## Concepts Kafka Streams Abordés
- Transformation de flux (`map()`)
- Filtrage (`filter()`)
- Agrégation (`groupBy()`, `reduce()`)

