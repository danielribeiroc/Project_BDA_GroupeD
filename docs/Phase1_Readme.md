
## Phase 1 - Selection du dataset
### Dataset 1: Stats bomb
- **Data** : https://github.com/statsbomb/open-data
- **Taille** : 10 GiB
- **Schema** : https://github.com/statsbomb/open-data/tree/master/doc
- **Datatype** : JSON

**Justification du choix du dataset**

Cette entreprise "StatsBomb" fournit des données de football de haute qualité, notamment des données de matchs, de joueurs, et de tactiques. 
Ce dataset est uniquement un extrait des données disponibles sur le site de StatsBomb. Malgré cela, il contient l'ensemble
des informations nécessaires pour réaliser des analyses avancées et développer des modèles prédictifs dans le domaine du football.

***Type d'analyse possible***
De nombreuses visualisations sont possibles: 
- Heatmap pour la possession de balle, pour la position d'un joueur, etc.
- Visualisation des tirs, les passes, les duels, etc. 
- Cartographie des actions des joueurs
- Visualisation de la position des joueurs sur le terrain en fonction du temps

Il est également possible de réaliser des analyses prédictives, par exemple:
- Prédiction de l'issue de matchs
- Estimation du nombre de cartons jaunes
- Classification des styles de jeu
- Prédiction de la direction d'une passe et de sa destination
- Prédiction du nombre de buts, nombre de tirs cadrés, etc.
- Prédiction de la position d'un joueur sur le terrain
- Prédiction de la position de la balle
- ...

***Pourquoi utiliser ce dataset (Statsbomb) ?***

Malgré la taille inférieure aux critères de ce projet, ce dataset reste très riche et n'est au final qu'un extrait du dataset réel, qui est beaucoup plus volumineux.
Dans notre cas, on a choisi ce dataset car on a une passion pour le football et travailler sur un dataset de football nous motive beaucoup. Il permet malgré tout de réaliser des analyses avancées et de développer des modèles prédictifs, tout en mettant en place l'infrastructure Big Data requise pour ce cours.

----------------------------------------------------------------
### Dataset 2: Binance Full History
- **Kaggle data** : https://www.kaggle.com/jorijnsmit/binance-full-history
- **Taille** : 34.59 GB
- **Schema** : 1 fichier Parquet par token
- **Datatype** : Parquet

***Type d'analyses***
- Prédiction sur des time series
- Analyse de corrélation

### Dataset 3: Taxi NYC
- **Kaggle data** : https://www.kaggle.com/datasets/microize/nyc-taxi-dataset
- **Taille** : 67.64 GB
- **Schema** : 1 fichier parquet par mois par type de taxi
- **Datatype** : Parquet

***Type d'analyses***
- Visualisation de la densité de taxis en fonction du temps, de la position des taxis en fonction du temps
- Prédiction sur des time series
- Analyse de corrélation