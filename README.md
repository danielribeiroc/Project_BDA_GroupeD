# BDA 2024 - Projet Big Data
Daniel Ribeiro, Ruben Terceiro, Killian Ruffieux

## Section 1: Sélection du Dataset
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



## Synthèse

Nous avons opté pour le dataset "StatsBomb" après une réflexion approfondie. Ce choix a été principalement motivé par notre intérêt pour le domaine du football et la richesse des données fournies par StatsBomb, qui permettent des analyses détaillées. Ce dataset, en offrant des données granulaires sur les matchs, les joueurs et les tactiques, est particulièrement adapté à notre ambition de modéliser et prédire les scores des matchs de football.

Notre objectif principal avec ce dataset sera de développer des modèles capables de prédire les résultats des matchs futurs en analysant les tendances et les performances des équipes au cours de la saison.

----------------------------------------------------------------

## Objectifs

Notre objectif principal est de prédire les scores des matchs en exploitant des features extraites du dataset. Ces features sélectionnées incluent:

- **ATWPAA VS HT**: Pourcentage de victoires de l'équipe extérieure contre l'équipe à domicile.
- **HTWPAH VS AT**: Pourcentage de victoires de l'équipe à domicile contre l'équipe extérieure.
- **H2H AW**: Résultats des confrontations directes antérieures avec victoire de l'équipe extérieure.
- **H2H HW**: Résultats des confrontations directes antérieures avec victoire de l'équipe à domicile.
- **DRAWN**: Historique des matchs nuls entre les deux équipes.
- **AT WNP** et **HT WNP**: Pourcentage de victoires de l'équipe extérieure et de l'équipe à domicile respectivement.
- **ATL2MWS** et **HTL2MWS**: Série de victoires des deux derniers matchs de l'équipe extérieure et de l'équipe à domicile.
- **AT GSPGAA** et **HT GSPGAA**: Buts marqués par match à l'extérieur pour l'équipe extérieure et à domicile pour l'équipe à domicile.
- **AT LSLP** et **HT LSLP**: Points obtenus dans la ligue lors de la saison précédente par l'équipe extérieure et l'équipe à domicile.

### Questions spécifiques sur les features et leur utilité

L'analyse de ces features nous aidera à comprendre les dynamiques sous-jacentes des matchs de football et à affiner nos prédictions. Voici les questions que nous explorerons et pourquoi elles sont cruciales :

- **Comment la performance historique à domicile et à l'extérieur (HTWPAH, ATWPAA) influence-t-elle les probabilités de résultats futurs ?**
  Cette analyse pourrait révéler des tendances importantes qui affectent les stratégies des équipes lors des matchs à domicile et à l'extérieur.
  
- **Dans quelle mesure les résultats des confrontations directes (H2H AW, H2H HW) sont-ils prédictifs des résultats de matchs futurs entre les mêmes équipes ?**
  Comprendre cette relation peut aider à prédire les issues des matchs basés sur l'historique des affrontements, ce qui est particulièrement utile dans les tournois récurrents.
  
- **Quelle est la corrélation entre les séries de victoires récentes (ATL2MWS, HTL2MWS) et la performance dans les matchs immédiatement suivants ?**
  Cela pourrait indiquer si l'élan ou la forme actuelle a un impact significatif sur les résultats des matchs.
  
- **Comment les scores moyens à domicile et à l'extérieur (AT GSPGAA, HT GSPGAA) peuvent-ils être utilisés pour prédire le score final d'un match ?**
  Analyser ces scores nous permettrait de comprendre l'impact des avantages à domicile et à l'extérieur sur les performances de scoring.
  
- **Quel est l'impact des performances de la saison précédente (AT LSLP, HT LSLP) sur les matchs de la saison en cours ?**
  Cette question aide à évaluer la constance des équipes d'une saison à l'autre et pourrait être utilisée pour identifier les tendances sur le long terme.

## Approche Méthodologique
### Définition de la Sémantique et Normalisation des Données

Avant toute analyse, nous définirons précisément la sémantique de chaque feature pour assurer une compréhension uniforme et éviter des erreurs d'interprétation. La normalisation des données sera ensuite effectuée pour standardiser les échelles de mesure, permettant ainsi des comparaisons et des analyses statistiques valides.

### Analyse Statistique et Modèles de Machine Learning

- **Analyse Exploratoire**: Utilisation de statistiques descriptives et de visualisations pour comprendre les distributions et les relations entre features.
- **Sélection de Features**: Application de techniques telles que l'analyse en composantes principales (PCA) pour réduire la dimensionnalité et focaliser sur les variables les plus influentes.
- **Modélisation Prédictive**: Développement de modèles de machine learning (algorithmes encore à définir) pour prédire les scores des matchs. Ces modèles seront entraînés, testés, et validés en utilisant des techniques de cross-validation pour évaluer leur précision et leur robustesse.

## Conclusion

En adoptant cette approche, nous espérons atteindre nos objectifs de prédiction des scores de match. Ce projet représente une excellente opportunité de combiner notre passion pour le football avec des compétences avancées en data science et en machine learning.