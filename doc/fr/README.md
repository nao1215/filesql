# filesql

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/filesql.svg)](https://pkg.go.dev/github.com/nao1215/filesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/filesql)](https://goreportcard.com/report/github.com/nao1215/filesql)
[![MultiPlatformUnitTest](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/filesql/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/filesql/coverage.svg)

[English](../../README.md) | [Русский](../ru/README.md) | [中文](../zh-cn/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [日本語](../ja/README.md)

![logo](../image/filesql-logo.png)

**filesql** est un pilote SQL Go qui vous permet d'interroger les fichiers CSV, TSV, LTSV, Parquet et Excel (XLSX) en utilisant la syntaxe SQL de SQLite3. Interrogez directement vos fichiers de données sans importation ou transformation !

**Vous voulez découvrir les capacités de filesql ?** Essayez **[sqly](https://github.com/nao1215/sqly)** - un outil en ligne de commande qui utilise filesql pour exécuter facilement des requêtes SQL sur les fichiers CSV, TSV, LTSV et Excel directement depuis votre shell ! C'est le moyen parfait de découvrir la puissance de filesql en action !

## 🎯 Pourquoi filesql ?

Cette bibliothèque est née de l'expérience de maintenir deux outils CLI séparés - [sqly](https://github.com/nao1215/sqly) et [sqluv](https://github.com/nao1215/sqluv). Les deux outils partageaient une caractéristique commune : exécuter des requêtes SQL sur les fichiers CSV, TSV et autres formats.

Plutôt que de maintenir du code dupliqué dans les deux projets, nous avons extrait la fonctionnalité principale dans ce pilote SQL réutilisable. Maintenant, tout développeur Go peut tirer parti de cette capacité dans ses propres applications !

## ✨ Fonctionnalités

- 🔍 **Interface SQL SQLite3** - Utilisez le puissant dialecte SQL de SQLite3 pour interroger vos fichiers
- 📁 **Formats de fichiers multiples** - Support pour les fichiers CSV, TSV, LTSV, Parquet et Excel (XLSX)
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
| `.parquet` | Parquet | Format columnaire Apache Parquet |
| `.xlsx` | Excel XLSX | Format de classeur Microsoft Excel |
| `.csv.gz`, `.tsv.gz`, `.ltsv.gz`, `.parquet.gz`, `.xlsx.gz` | Compression Gzip | Fichiers compressés Gzip |
| `.csv.bz2`, `.tsv.bz2`, `.ltsv.bz2`, `.parquet.bz2`, `.xlsx.bz2` | Compression Bzip2 | Fichiers compressés Bzip2 |
| `.csv.xz`, `.tsv.xz`, `.ltsv.xz`, `.parquet.xz`, `.xlsx.xz` | Compression XZ | Fichiers compressés XZ |
| `.csv.zst`, `.tsv.zst`, `.ltsv.zst`, `.parquet.zst`, `.xlsx.zst` | Compression Zstandard | Fichiers compressés Zstandard |

## 📦 Installation

```bash
go get github.com/nao1215/filesql
```

## 🔧 Configuration requise

- **Version Go**: 1.24 ou ultérieure
- **Systèmes d'exploitation supportés**:
  - Linux
  - macOS  
  - Windows

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

// Ouvrir plusieurs fichiers à la fois (incluant Parquet)
db, err := filesql.OpenContext(ctx, "users.csv", "orders.tsv", "logs.ltsv.gz", "analytics.parquet")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Joindre les données de différents formats de fichiers
rows, err := db.QueryContext(ctx, `
    SELECT u.name, o.order_date, l.event, a.metrics
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN logs l ON u.id = l.user_id
    JOIN analytics a ON u.id = a.user_id
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
        SetDefaultChunkSize(5000). // 5000 lignes par chunk
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

// Exporter au format Parquet (lorsque disponible)
parquetOptions := filesql.NewDumpOptions().
    WithFormat(filesql.OutputFormatParquet)
// Note: L'exportation Parquet est implémentée (compression externe non supportée, utilisez la compression intégrée de Parquet)
```

## 📝 Règles de nommage des tables

filesql dérive automatiquement les noms de tables des chemins de fichiers :

- `users.csv` → table `users`
- `data.tsv.gz` → table `data`
- `/path/to/sales.csv` → table `sales`
- `products.ltsv.bz2` → table `products`
- `analytics.parquet` → table `analytics`

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
- Configurez les tailles de chunk (lignes par chunk) avec `SetDefaultChunkSize()` pour l'optimisation mémoire
- Une seule connexion SQLite fonctionne mieux pour la plupart des scénarios
- Utilisez le streaming pour les fichiers plus grands que la mémoire disponible

### Limitations de concurrence
⚠️ **IMPORTANT** : Cette bibliothèque **N'EST PAS thread-safe** et a des **limitations de concurrence** :
- **NE** partagez **PAS** les connexions de base de données entre les goroutines
- **NE** effectuez **PAS** d'opérations concurrentes sur la même instance de base de données
- **NE** appelez **PAS** `db.Close()` pendant que des requêtes sont actives dans d'autres goroutines
- Utilisez des instances de base de données séparées pour les opérations concurrentes si nécessaire
- Les conditions de course peuvent causer des fautes de segmentation ou une corruption de données

**Modèle recommandé pour l'accès concurrent** :
```go
// ✅ BON : Instances de base de données séparées par goroutine
func processFileConcurrently(filename string) error {
    db, err := filesql.Open(filename)  // Chaque goroutine obtient sa propre instance
    if err != nil {
        return err
    }
    defer db.Close()
    
    // Sûr à utiliser dans cette goroutine
    return processData(db)
}

// ❌ MAUVAIS : Partager une instance de base de données entre les goroutines
var sharedDB *sql.DB  // Cela causera des conditions de course
```

### Support Parquet
- **Lecture** : Support complet pour les fichiers Apache Parquet avec des types de données complexes
- **Écriture** : La fonctionnalité d'exportation est implémentée (compression externe non supportée, utilisez la compression intégrée de Parquet)
- **Mappage des types** : Les types Parquet sont mappés vers les types SQLite
- **Compression** : La compression intégrée de Parquet est utilisée au lieu de la compression externe
- **Gros volumes de données** : Les fichiers Parquet sont traités efficacement avec le format columnaire d'Arrow

### Support Excel (XLSX)
- **Structure 1-feuille-1-table** : Chaque feuille d'un classeur Excel devient une table SQL séparée
- **Nommage des tables** : Les noms de tables SQL suivent le format `{nomfichier}_{nomfeuille}` (ex., "ventes_T1", "ventes_T2")
- **Traitement des en-têtes** : La première ligne de chaque feuille devient les en-têtes de colonnes pour cette table
- **Opérations SQL standard** : Interrogez chaque feuille indépendamment ou utilisez des JOIN pour combiner les données entre les feuilles
- **Exigences mémoire** : Les fichiers XLSX nécessitent un chargement complet en mémoire en raison de la structure du format basé sur ZIP, même lors des opérations de streaming
- **Chargement complet en mémoire** : Les fichiers XLSX sont entièrement chargés en mémoire en raison de leur structure ZIP, et toutes les feuilles sont traitées (pas seulement la première). Les analyseurs en streaming CSV/TSV ne s'appliquent pas aux fichiers XLSX
- **Fonctionnalité d'exportation** : Lors de l'exportation au format XLSX, les noms de tables deviennent automatiquement des noms de feuilles
- **Support de compression** : Support complet pour les fichiers XLSX compressés (.xlsx.gz, .xlsx.bz2, .xlsx.xz, .xlsx.zst)

#### Exemple de structure de fichier Excel
```
Fichier Excel avec plusieurs feuilles :

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Feuille1    │    │ Feuille2    │    │ Feuille3    │
│ Nom    Âge  │    │ Produit     │    │ Région      │
│ Alice   25  │    │ Portable    │    │ Nord        │
│ Pierre  30  │    │ Souris      │    │ Sud         │
└─────────────┘    └─────────────┘    └─────────────┘

Résulte en 3 tables SQL séparées :

ventes_Feuille1:        ventes_Feuille2:        ventes_Feuille3:
┌──────┬─────┐          ┌─────────┐             ┌────────┐
│ Nom  │ Âge │          │ Produit │             │ Région │
├──────┼─────┤          ├─────────┤             ├────────┤
│ Alice│  25 │          │Portable │             │ Nord   │
│Pierre│  30 │          │ Souris  │             │ Sud    │
└──────┴─────┘          └─────────┘             └────────┘

Exemples SQL :
SELECT * FROM ventes_Feuille1 WHERE Âge > 27;
SELECT f1.Nom, f2.Produit FROM ventes_Feuille1 f1 
  JOIN ventes_Feuille2 f2 ON f1.rowid = f2.rowid;
```

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