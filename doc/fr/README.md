# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

**filesql** est un pilote SQL pour Go qui vous permet d'interroger les fichiers CSV, TSV et LTSV en utilisant la syntaxe SQL de SQLite3. Interrogez vos fichiers de données directement sans importation ou transformation !

## 🎯 Pourquoi filesql ?

Cette bibliothèque est née de l'expérience de maintenir deux outils CLI séparés - [sqly](https://github.com/nao1215/sqly) et [sqluv](https://github.com/nao1215/sqluv). Les deux outils partageaient une fonctionnalité commune : exécuter des requêtes SQL sur des fichiers CSV, TSV et d'autres formats de fichiers.

Plutôt que de maintenir du code dupliqué dans les deux projets, nous avons extrait la fonctionnalité centrale dans ce pilote SQL réutilisable. Désormais, tout développeur Go peut exploiter cette capacité dans ses propres applications !

## ✨ Fonctionnalités

- 🔍 **Interface SQL SQLite3** - Utilisez le puissant dialecte SQL de SQLite3 pour interroger vos fichiers
- 📁 **Formats de fichiers multiples** - Prise en charge des fichiers CSV, TSV et LTSV
- 🗜️ **Support de compression** - Gère automatiquement les fichiers compressés .gz, .bz2, .xz et .zst
- 🚀 **Configuration zéro** - Aucun serveur de base de données requis, tout fonctionne en mémoire
- 🌍 **Multi-plateforme** - Fonctionne parfaitement sur Linux, macOS et Windows
- 💾 **Alimenté par SQLite3** - Construit sur le moteur SQLite3 robuste pour un traitement SQL fiable

## 📋 Formats de fichiers supportés

| Extension | Format | Description |
|-----------|--------|-------------|
| `.csv` | CSV | Valeurs séparées par des virgules |
| `.tsv` | TSV | Valeurs séparées par des tabulations |
| `.ltsv` | LTSV | Valeurs étiquetées séparées par des tabulations |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz` | Compression Gzip | Fichiers compressés Gzip |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2` | Compression Bzip2 | Fichiers compressés Bzip2 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz` | Compression XZ | Fichiers compressés XZ |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst` | Compression Zstandard | Fichiers compressés Zstandard |


## 📦 Installation

```bash
go get github.com/nao1215/filesql
```

## 🚀 Démarrage rapide

[Le code d'exemple est ici](../../example_test.go).

### Usage simple (Fichiers)

Pour un accès simple aux fichiers, utilisez les fonctions pratiques `Open` ou `OpenContext` :

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "github.com/nao1215/filesql"
)

func main() {
    // Ouvrir un fichier CSV comme une base de données avec contexte
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Exécuter une requête SQL (le nom de table est dérivé du nom de fichier sans extension)
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25 ORDER BY name")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // Traiter les résultats
    for rows.Next() {
        var name string
        var age int
        if err := rows.Scan(&name, &age); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("Name: %s, Age: %d\n", name, age)
    }
}
```

### Patron Builder (Requis pour fs.FS)

Pour les cas d'usage avancés comme les fichiers intégrés (`go:embed`) ou les systèmes de fichiers personnalisés, utilisez le **patron Builder** :

```go
package main

import (
    "context"
    "embed"
    "io/fs"
    "log"
    
    "github.com/nao1215/filesql"
)

//go:embed data/*.csv data/*.tsv
var dataFS embed.FS

func main() {
    ctx := context.Background()
    
    // Utiliser le patron Builder pour le système de fichiers intégré
    subFS, _ := fs.Sub(dataFS, "data")
    
    db, err := filesql.NewBuilder().
        AddPath("local_file.csv").  // Fichier régulier
        AddFS(subFS).               // Système de fichiers intégré
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    connection, err := db.Open(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer connection.Close()
    defer db.Cleanup() // Nettoyer les fichiers temporaires du FS
    
    // Requête à travers des fichiers de sources différentes
    rows, err := connection.Query("SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // Traiter les résultats...
}
```

### Ouvrir avec support du contexte

```go
// Ouvrir des fichiers avec contrôle du délai d'expiration
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "large_dataset.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Requête avec contexte pour le support d'annulation
rows, err := db.QueryContext(ctx, "SELECT * FROM large_dataset WHERE status = 'active'")
```

### Ouvrir plusieurs fichiers

```go
// Ouvrir plusieurs fichiers dans une seule base de données
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "products.ltsv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Joindre des données de différents formats de fichiers !
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, p.product_name
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN products p ON o.product_id = p.id
    WHERE o.order_date > '2024-01-01'
`)
```

### Travailler avec les répertoires

```go
// Ouvrir tous les fichiers supportés dans un répertoire (récursivement)
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Interroger toutes les tables chargées
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

### Support des fichiers compressés

```go
// Gère automatiquement les fichiers compressés
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "large_dataset.csv.gz", "archive.tsv.bz2")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Interroger les données compressées de manière transparente
rows, err := db.QueryContext(ctx, "SELECT COUNT(*) FROM large_dataset")
```

### Règles de nommage des tables

filesql dérive automatiquement les noms de tables des chemins de fichiers :

```go
// Exemples de nommage de tables :
// "users.csv"           -> nom de table : "users"
// "data.tsv"            -> nom de table : "data"
// "logs.ltsv"           -> nom de table : "logs"
// "archive.csv.gz"      -> nom de table : "archive"
// "backup.tsv.bz2"      -> nom de table : "backup"
// "/path/to/sales.csv"  -> nom de table : "sales"

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.tsv.gz")
if err != nil {
    log.Fatal(err)
}

// Utiliser les noms de tables dérivés dans les requêtes
rows, err := db.QueryContext(ctx, `
    SELECT * FROM employees 
    JOIN departments ON employees.dept_id = departments.id
`)
```

## ⚠️ Notes importantes

### Syntaxe SQL
Étant donné que filesql utilise SQLite3 comme moteur sous-jacent, toute la syntaxe SQL suit le [dialecte SQL de SQLite3](https://www.sqlite.org/lang.html). Cela inclut :
- Fonctions (p. ex., `date()`, `substr()`, `json_extract()`)
- Fonctions de fenêtre
- Expressions de table communes (CTE)
- Et bien plus encore !

### Modifications de données
- Les opérations `INSERT`, `UPDATE` et `DELETE` n'affectent que la base de données en mémoire
- **Les fichiers originaux restent inchangés** - filesql ne modifie jamais vos fichiers sources
- Cela rend l'expérimentation avec les transformations de données sûre

### Fonctionnalités SQL avancées

Étant donné que filesql utilise SQLite3, vous pouvez exploiter toute sa puissance :

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Utiliser des fonctions de fenêtre, CTE et des requêtes complexes
query := `
    WITH dept_stats AS (
        SELECT 
            department_id,
            AVG(salary) as avg_salary,
            COUNT(*) as emp_count
        FROM employees
        GROUP BY department_id
    )
    SELECT 
        e.name,
        e.salary,
        d.name as department,
        ds.avg_salary as dept_avg,
        RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) as rank
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    JOIN dept_stats ds ON e.department_id = ds.department_id
    WHERE e.salary > ds.avg_salary * 0.8
`

rows, err := db.QueryContext(ctx, query)
```

### Exporter des données modifiées

Si vous devez persister les modifications apportées à la base de données en mémoire :

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Effectuer des modifications
_, err = db.ExecContext(ctx, "UPDATE data SET status = 'processed' WHERE status = 'pending'")
if err != nil {
    log.Fatal(err)
}

// Exporter les données modifiées vers un nouveau répertoire
err = filesql.DumpDatabase(db, "/path/to/output/directory")
if err != nil {
    log.Fatal(err)
}
```

## 🤝 Contribuer

Les contributions sont les bienvenues ! Veuillez consulter le [Guide de Contribution](CONTRIBUTING.md) pour plus de détails.

## 💖 Soutien

Si vous trouvez ce projet utile, veuillez considérer :

- ⭐ Lui donner une étoile sur GitHub - cela aide les autres à découvrir le projet
- 💝 [Devenir sponsor](https://github.com/sponsors/nao1215) - votre soutien maintient le projet vivant et motive le développement continu

Votre soutien, que ce soit par des étoiles, des parrainages ou des contributions, est ce qui fait avancer ce projet. Merci !

## 📄 Licence

Ce projet est sous licence MIT - voir le fichier [LICENSE](../../LICENSE) pour plus de détails.