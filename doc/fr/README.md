# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [Русский](../ru/README.md) | [中文](../zh-cn/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [日本語](../ja/README.md)

![logo](../image/filesql-logo.png)

**filesql** est un pilote SQL Go qui vous permet d'interroger les fichiers CSV, TSV et LTSV en utilisant la syntaxe SQL de SQLite3. Interrogez directement vos fichiers de données sans importation ou transformation !

## 🎯 Pourquoi filesql ?

Cette bibliothèque est née de l'expérience de maintenir deux outils CLI séparés - [sqly](https://github.com/nao1215/sqly) et [sqluv](https://github.com/nao1215/sqluv). Les deux outils partageaient une caractéristique commune : exécuter des requêtes SQL sur les fichiers CSV, TSV et autres formats.

Plutôt que de maintenir du code dupliqué dans les deux projets, nous avons extrait la fonctionnalité principale dans ce pilote SQL réutilisable. Maintenant, tout développeur Go peut tirer parti de cette capacité dans ses propres applications !

## ✨ Fonctionnalités

- 🔍 **Interface SQL SQLite3** - Utilisez le puissant dialecte SQL de SQLite3 pour interroger vos fichiers
- 📁 **Formats de fichiers multiples** - Support pour les fichiers CSV, TSV et LTSV
- 🗜️ **Support de compression** - Gère automatiquement les fichiers compressés .gz, .bz2, .xz et .zst
- 🌊 **Traitement en flux** - Gère efficacement les gros fichiers grâce au streaming avec des tailles de chunk configurables
- 📖 **Sources d'entrée flexibles** - Support pour les chemins de fichiers, répertoires, io.Reader et embed.FS
- 🚀 **Configuration zéro** - Aucun serveur de base de données requis, tout fonctionne en mémoire
- 💾 **Sauvegarde automatique** - Persiste automatiquement les modifications dans les fichiers
- 🌍 **Multi-plateforme** - Fonctionne parfaitement sur Linux, macOS et Windows
- ⚡ **Propulsé par SQLite3** - Construit sur le moteur SQLite3 robuste pour un traitement SQL fiable

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

### Usage simple

La façon recommandée de commencer est avec `OpenContext` pour une gestion appropriée des timeouts :

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
    // Créer un contexte avec timeout pour les opérations sur gros fichiers
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // Ouvrir un fichier CSV comme base de données
    db, err := filesql.OpenContext(ctx, "data.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Interroger les données (nom de table = nom de fichier sans extension)
    rows, err := db.QueryContext(ctx, "SELECT * FROM data WHERE age > 25")
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
        fmt.Printf("Nom: %s, Âge: %d\n", name, age)
    }
}
```

### Fichiers multiples et formats

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Ouvrir plusieurs fichiers à la fois
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Joindre les données de différents formats de fichiers
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
    WHERE o.order_date > '2024-01-01'
`)
```

### Travailler avec des répertoires

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Charger tous les fichiers supportés d'un répertoire (récursivement)
db, err := filesql.OpenContext(ctx, "/path/to/data/directory")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Voir quelles tables sont disponibles
rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
```

## 🔧 Usage avancé

### Motif Builder

Pour les scénarios avancés, utilisez le motif builder :

```go
package main

import (
    "context"
    "embed"
    "log"
    
    "github.com/nao1215/filesql"
)

//go:embed data/*.csv
var embeddedFiles embed.FS

func main() {
    ctx := context.Background()
    
    // Configurer les sources de données avec le builder
    validatedBuilder, err := filesql.NewBuilder().
        AddPath("local_file.csv").      // Fichier local
        AddFS(embeddedFiles).           // Fichiers intégrés
        SetDefaultChunkSize(50*1024*1024). // Chunks de 50MB
        Build(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    db, err := validatedBuilder.Open(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Interroger toutes les sources de données
    rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
}
```

### Fonctionnalités de sauvegarde automatique

#### Sauvegarde automatique à la fermeture de la base de données

```go
// Sauvegarder automatiquement les modifications à la fermeture de la base de données
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSave("./backup"). // Sauvegarder dans le répertoire de sauvegarde
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close() // Les modifications sont automatiquement sauvegardées ici

// Effectuer des modifications
db.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
db.Exec("INSERT INTO data (name, age) VALUES ('Jean', 30)")
```

#### Sauvegarde automatique lors du commit de transaction

```go
// Sauvegarder automatiquement après chaque transaction
validatedBuilder, err := filesql.NewBuilder().
    AddPath("data.csv").
    EnableAutoSaveOnCommit(""). // Vide = écraser les fichiers originaux
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Les modifications sont sauvegardées après chaque commit
tx, _ := db.Begin()
tx.Exec("UPDATE data SET status = 'processed' WHERE id = 1")
tx.Commit() // La sauvegarde automatique se produit ici
```

### Travailler avec io.Reader et données réseau

```go
import (
    "net/http"
    "github.com/nao1215/filesql"
)

// Charger des données depuis une réponse HTTP
resp, err := http.Get("https://example.com/data.csv")
if err != nil {
    log.Fatal(err)
}
defer resp.Body.Close()

validatedBuilder, err := filesql.NewBuilder().
    AddReader(resp.Body, "remote_data", filesql.FileTypeCSV).
    Build(ctx)
if err != nil {
    log.Fatal(err)
}

db, err := validatedBuilder.Open(ctx)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Interroger les données distantes
rows, err := db.QueryContext(ctx, "SELECT * FROM remote_data LIMIT 10")
```

### Exportation manuelle de données

Si vous préférez un contrôle manuel sur la sauvegarde :

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "data.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Effectuer des modifications
db.Exec("UPDATE data SET status = 'processed'")

// Exporter manuellement les modifications
err = filesql.DumpDatabase(db, "./output")
if err != nil {
    log.Fatal(err)
}

// Ou avec format et compression personnalisés
options := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatTSV).
    WithCompression(filesql.CompressionGZ)
err = filesql.DumpDatabase(db, "./output", options)
```

## 📝 Règles de nommage des tables

filesql dérive automatiquement les noms de tables des chemins de fichiers :

- `users.csv` → table `users`
- `data.tsv.gz` → table `data`
- `/path/to/sales.csv` → table `sales`
- `products.ltsv.bz2` → table `products`

## ⚠️ Notes importantes

### Syntaxe SQL
Puisque filesql utilise SQLite3 comme moteur sous-jacent, toute la syntaxe SQL suit le [dialecte SQL de SQLite3](https://www.sqlite.org/lang.html). Cela inclut :
- Fonctions (ex., `date()`, `substr()`, `json_extract()`)
- Fonctions de fenêtre
- Expressions de table communes (CTE)
- Déclencheurs et vues

### Modifications de données
- Les opérations `INSERT`, `UPDATE` et `DELETE` affectent la base de données en mémoire
- **Les fichiers originaux restent inchangés par défaut**
- Utilisez les fonctionnalités de sauvegarde automatique ou `DumpDatabase()` pour persister les modifications
- Cela rend sûr l'expérimentation avec les transformations de données

### Conseils de performance
- Utilisez `OpenContext()` avec des timeouts pour les gros fichiers
- Configurez les tailles de chunk avec `SetDefaultChunkSize()` pour l'optimisation mémoire
- Une seule connexion SQLite fonctionne mieux pour la plupart des scénarios
- Utilisez le streaming pour les fichiers plus grands que la mémoire disponible

## 🎨 Exemples avancés

### Requêtes SQL complexes

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

db, err := filesql.OpenContext(ctx, "employees.csv", "departments.csv")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Utiliser les fonctionnalités avancées de SQLite
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
        RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) as salary_rank
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    JOIN dept_stats ds ON e.department_id = ds.department_id
    WHERE e.salary > ds.avg_salary * 0.8
    ORDER BY d.name, salary_rank
`

rows, err := db.QueryContext(ctx, query)
```

### Contexte et annulation

```go
import (
    "context"
    "time"
)

// Définir un timeout pour les opérations sur gros fichiers
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

db, err := filesql.OpenContext(ctx, "huge_dataset.csv.gz")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Requête avec contexte pour le support d'annulation
rows, err := db.QueryContext(ctx, "SELECT * FROM huge_dataset WHERE status = 'active'")
```

## 🤝 Contribuer

Les contributions sont bienvenues ! Veuillez consulter le [Guide de Contribution](../../CONTRIBUTING.md) pour plus de détails.

## 💖 Support

Si vous trouvez ce projet utile, veuillez considérer :

- ⭐ Lui donner une étoile sur GitHub - cela aide les autres à découvrir le projet
- 💝 [Devenir sponsor](https://github.com/sponsors/nao1215) - votre support maintient le projet vivant et motive le développement continu

Votre support, que ce soit par des étoiles, des parrainages ou des contributions, est ce qui fait avancer ce projet. Merci !

## 📄 Licence

Ce projet est sous licence MIT - consultez le fichier [LICENSE](../../LICENSE) pour plus de détails.