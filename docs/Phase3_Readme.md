
## Phase 3 - Plannification - Description des features et des questions analytiques

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

Il est important de mentionner que ces features définis peuvent changer en fonction de l'analyse exploratoire des données et de la sélection de features. Les features finaux seront déterminés après une analyse approfondie des données et donc définitifs dans la phase 4 et finale du projet.

### Questions analytiques

  L'analyse de ces points nous aidera à comprendre les dynamiques sous-jacentes des matchs de football et à affiner nos prédictions. Voici les questions que nous explorons et pourquoi elles sont cruciales :

- **Est-ce que notre modèle permet de prédire les résultats d'un match ?** 
  Il s'agit ici de la question principale de notre projet. Celle-ci déterminera l'efficacité de notre modèle ainsi que sa pertinence.

- **Quelle est la fréquence des victoires à domicile par rapport aux victoires à l'extérieur ?**
  Cette question vise à identifier les tendances générales dans les résultats des matchs pour voir si jouer à domicile peut être un avantage significatif.

- **Comment les scores moyens des équipes varient-ils entre les matchs à domicile et les matchs à l'extérieur ?**
  Répondre à cette question nous aidera à savoir si une équipe a des performances plus réglière lorsqu'elle joue à domicile.
  
- **(Optionnel) Y a-t-il une corrélation entre le nombre de buts marqués et le résultat final du match ?**
  Cela pourrait indiquer que si une équipe à marqué beaucoup de buts au cours d'une saison qu'elle a plus de chance de gagner des matchs.
  
- **(Optionnel) Quelle est l'importance de la série de victoires actuelle d'une équipe pour prédire le résultat de son prochain match ?**
  Cette question aide à évaluer si les performances passées peuvent être des indicateurs fiables pour les performances futures

  Les 2 dernières questions notées comme optionnelles seront traitées en fonction du temps à disposition.

## Approche Méthodologique
### Définition de la Sémantique et Normalisation des Données

Avant toute analyse, nous définirons précisément la sémantique de chaque feature pour assurer une compréhension uniforme et éviter des erreurs d'interprétation. La normalisation des données sera ensuite effectuée pour standardiser les échelles de mesure, permettant ainsi des comparaisons et des analyses statistiques valides.

### Analyse Statistique et Modèles de Machine Learning

- **Analyse Exploratoire**: Utilisation de statistiques descriptives et de visualisations pour comprendre les distributions et les relations entre features. On a pour but de garantir et de voir si les features sélectionnées sont pertinentes pour la prédiction des scores. Une chose importante sera de savoir si on a un nombre suffisant de données pour chaque feature.
- **Sélection de Features**: Application de techniques telles que l'analyse en composantes principales (PCA) pour réduire la dimensionnalité et focaliser sur les variables les plus influentes.
- **Modélisation Prédictive**: Développement de modèles de machine learning (algorithmes encore à définir) pour prédire les scores des matchs. Ces modèles seront entraînés, testés, et validés en utilisant des techniques de cross-validation pour évaluer leur précision et leur robustesse.

## Conclusion

En adoptant cette approche, nous espérons atteindre nos objectifs de prédiction des scores de match. Ce projet représente une excellente opportunité de combiner notre passion pour le football avec des compétences avancées en data science et en machine learning.