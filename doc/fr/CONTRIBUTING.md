# Guide de Contribution

## Introduction

Merci d'envisager de contribuer au projet filesql ! Ce document explique comment contribuer au projet. Nous accueillons toutes les formes de contributions, y compris les contributions de code, les améliorations de documentation, les rapports de bogues et les suggestions de fonctionnalités.

## Configuration de l'Environnement de Développement

### Prérequis

#### Installation de Go

Le développement de filesql nécessite Go 1.24 ou une version ultérieure.

**macOS (avec Homebrew)**
```bash
brew install go
```

**Linux (pour Ubuntu)**
```bash
# Avec snap
sudo snap install go --classic

# Ou télécharger depuis le site officiel
wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile
source ~/.profile
```

**Windows**
Téléchargez et exécutez l'installateur depuis le [site officiel de Go](https://go.dev/dl/).

Vérifier l'installation :
```bash
go version
```

### Cloner le Projet

```bash
git clone https://github.com/nao1215/filesql.git
cd filesql
```

### Installer les Outils de Développement

```bash
# Installer les outils de développement nécessaires
make tools
```

### Vérification

Pour vérifier que votre environnement de développement est correctement configuré, exécutez les commandes suivantes :

```bash
# Exécuter les tests
make test

# Exécuter le linter
make lint
```

## Structure du Projet

```
filesql/
├── domain/          # Couche du modèle de domaine
│   ├── model/      # Définitions du modèle de domaine
│   └── repository/ # Interfaces du dépôt
├── driver/         # Implémentation du pilote SQLite
├── testdata/       # Fichiers de données de test
├── doc/            # Documentation
│   ├── ja/        # Documentation japonaise
│   ├── zh-cn/     # Documentation chinoise
│   ├── es/        # Documentation espagnole
│   └── ...        # Autres langues
├── filesql.go      # Point d'entrée principal de la bibliothèque
├── filesql_test.go # Tests de la bibliothèque
└── example_test.go # Exemples d'utilisation
```

### Rôles des Répertoires

- **domain/** : Couche contenant la logique métier et les modèles de domaine. Implémentation Go pure sans dépendances externes
- **driver/** : Interface et implémentation du pilote SQLite. Fournit un pilote compatible avec database/sql
- **testdata/** : Fichiers exemples tels que CSV, TSV, LTSV utilisés dans les tests
- **doc/** : Documentation multilingue avec des sous-répertoires pour chaque langue

## Flux de Travail de Développement

### Stratégie de Branches

- La branche `main` est la dernière version stable
- Créez de nouvelles branches depuis `main` pour les nouvelles fonctionnalités ou les corrections de bogues
- Exemples de noms de branches :
  - `feature/add-json-support` - Nouvelle fonctionnalité
  - `fix/issue-123` - Correction de bogue
  - `docs/update-readme` - Mise à jour de la documentation

### Standards de Codage

Ce projet suit ces standards :

1. **Se conformer à [Effective Go](https://go.dev/doc/effective_go)**
2. **Éviter l'utilisation de variables globales** (sauf pour le package config)
3. **Toujours ajouter des commentaires aux fonctions, variables et structures publiques**
4. **Garder les fonctions aussi petites que possible**
5. **L'écriture de tests est encouragée**

### Écrire des Tests

Les tests sont importants. Veuillez suivre ces directives :

1. **Tests unitaires** : Viser une couverture de 80% ou plus
2. **Lisibilité des tests** : Écrire des cas de test clairs
3. **Exécution parallèle** : Utiliser `t.Parallel()` autant que possible

Exemple de test :
```go
func TestFile_Parse(t *testing.T) {
    t.Parallel()
    
    t.Run("should parse CSV file correctly", func(t *testing.T) {
        // Entrée et valeurs attendues claires pour le cas de test
        input := "name,age\nAlice,30"
        expected := &Table{...}
        
        result, err := ParseCSV(input)
        assert.NoError(t, err)
        assert.Equal(t, expected, result)
    })
}
```

## Créer des Pull Requests

### Préparation

1. **Vérifier ou Créer des Issues**
   - Vérifiez s'il existe des issues existantes
   - Pour les changements majeurs, nous recommandons de discuter de l'approche dans une issue d'abord

2. **Écrire des Tests**
   - Toujours ajouter des tests pour les nouvelles fonctionnalités
   - Pour les corrections de bogues, créez des tests qui reproduisent le bogue

3. **Contrôle de Qualité**
   ```bash
   # S'assurer que tous les tests passent
   make test
   
   # Vérification du linter
   make lint
   
   # Vérifier la couverture (80% ou plus)
   go test -cover ./...
   ```

### Soumettre une Pull Request

1. Créez une Pull Request depuis votre dépôt forké vers le dépôt principal
2. Le titre de la PR doit décrire brièvement les changements
3. Incluez ce qui suit dans la description de la PR :
   - Objectif et contenu des changements
   - Numéro d'issue associé (le cas échéant)
   - Méthode de test
   - Étapes de reproduction pour les corrections de bogues

### À Propos de CI/CD

GitHub Actions vérifie automatiquement les éléments suivants :

- **Tests multiplateforme** : Exécution des tests sur Linux, macOS et Windows
- **Vérification du linter** : Analyse statique avec golangci-lint
- **Couverture des tests** : Maintenir une couverture de 80% ou plus
- **Vérification de compilation** : Compilations réussies sur chaque plateforme

La fusion n'est pas possible tant que toutes les vérifications ne sont pas passées.

## Rapports de Bogues

Lorsque vous trouvez un bogue, veuillez créer une issue avec les informations suivantes :

1. **Informations sur l'Environnement**
   - OS (Linux/macOS/Windows) et version
   - Version de Go
   - Version de filesql

2. **Étapes de Reproduction**
   - Exemple de code minimal pour reproduire le bogue
   - Fichiers de données utilisés (si possible)

3. **Comportement Attendu et Réel**

4. **Messages d'Erreur ou Traces de Pile** (le cas échéant)

## Contribuer en Dehors du Code

Les activités suivantes sont également très bienvenues :

### Activités qui Stimulent la Motivation

- **Donner une Étoile GitHub** : Montrez votre intérêt pour le projet
- **Promouvoir le Projet** : Le présenter dans des blogs, réseaux sociaux, groupes d'étude, etc.
- **Devenir un Sponsor GitHub** : Support disponible sur [https://github.com/sponsors/nao1215](https://github.com/sponsors/nao1215)

### Autres Façons de Contribuer

- **Améliorations de la Documentation** : Corriger les fautes de frappe, améliorer la clarté des explications
- **Traductions** : Traduire la documentation dans de nouvelles langues
- **Ajouter des Exemples** : Fournir du code d'exemple pratique
- **Suggestions de Fonctionnalités** : Partager des idées de nouvelles fonctionnalités dans les issues

## Communauté

### Code de Conduite

Veuillez vous référer à [CODE_OF_CONDUCT.md](../../CODE_OF_CONDUCT.md). Nous attendons de tous les contributeurs qu'ils se traitent avec respect.

### Questions et Rapports

- **GitHub Issues** : Rapports de bogues et suggestions de fonctionnalités

## Licence

Les contributions à ce projet sont considérées comme publiées sous la licence du projet (Licence MIT).

---

Merci encore d'envisager de contribuer ! Nous attendons sincèrement votre participation.