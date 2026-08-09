//nolint:gosec // Example files use simplified temporary file handling for clarity.
package frame_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/nao1215/filesql/frame"
)

func createFrameExampleFile(body string) string {
	dir, err := os.MkdirTemp("", "frame-example")
	if err != nil {
		log.Fatal(err)
	}
	path := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(path, []byte(strings.TrimLeft(body, "\n")), 0600); err != nil {
		log.Fatal(err)
	}
	return path
}

func ExampleNewDataFrame() {
	df, err := frame.NewDataFrame(strings.NewReader("name,qty\napple,3\nbanana,2\n"), frame.CSV)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%v %d\n", df.Columns(), df.Len())
	// Output:
	// [name qty] 2
}

func ExampleNewDataFrameFromPath() {
	path := createFrameExampleFile("name,qty\napple,3\nbanana,2\n")
	defer os.RemoveAll(filepath.Dir(path))

	df, err := frame.NewDataFrameFromPath(path)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(df.Len())
	// Output:
	// 2
}

func ExampleNewDataFrameFromRecords() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"name": "apple", "qty": 3},
		{"name": "banana", "qty": 2},
	})

	fmt.Println(df.Columns())
	// Output:
	// [name qty]
}

func ExampleDataFrame_ToCSV() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"name": "apple", "qty": 3},
	})

	dir, err := os.MkdirTemp("", "frame-csv")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "out.csv")
	if err := df.ToCSV(path); err != nil {
		log.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(string(data))
	// Output:
	// name,qty
	// apple,3
}

func ExampleDataFrame_ToTSV() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"name": "apple", "qty": 3},
	})

	dir, err := os.MkdirTemp("", "frame-tsv")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "out.tsv")
	if err := df.ToTSV(path); err != nil {
		log.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(string(data))
	// Output:
	// name	qty
	// apple	3
}

func ExampleDataFrame_Select() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"name": "apple", "qty": 3, "region": "north"},
	})

	selected, _ := df.Select("name", "region") //nolint:errcheck // the columns exist
	fmt.Println(selected.Columns())
	// Output:
	// [name region]
}

func ExampleDataFrame_Filter() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"name": "apple", "qty": int64(3)},
		{"name": "banana", "qty": int64(1)},
	})

	filtered := df.Filter(func(row map[string]any) bool {
		qty, ok := row["qty"].(int64)
		if !ok {
			log.Fatalf("qty has type %T, want int64", row["qty"])
		}
		return qty >= 2
	})

	fmt.Println(filtered.ToRecords()[0]["name"])
	// Output:
	// apple
}

func ExampleDataFrame_Mutate() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"product": "apple", "qty": int64(3), "price": int64(4)},
	})

	withTotal := df.Mutate("total", func(row map[string]any) any {
		qty, ok := row["qty"].(int64)
		if !ok {
			log.Fatalf("qty has type %T, want int64", row["qty"])
		}
		price, ok := row["price"].(int64)
		if !ok {
			log.Fatalf("price has type %T, want int64", row["price"])
		}
		return qty * price
	})

	fmt.Println(withTotal.ToRecords()[0]["total"])
	// Output:
	// 12
}

func ExampleDataFrame_Join() {
	users := frame.NewDataFrameFromRecords([]map[string]any{
		{"user_id": 1, "name": "Alice"},
		{"user_id": 2, "name": "Bob"},
	})
	orders := frame.NewDataFrameFromRecords([]map[string]any{
		{"customer_id": 1, "item": "laptop"},
	})

	joined, err := users.Join(orders, frame.JoinOption{
		On:  []string{"user_id", "customer_id"},
		How: frame.LeftJoin,
	})
	if err != nil {
		log.Fatal(err)
	}

	row := joined.ToRecords()[0]
	fmt.Printf("%s %s\n", row["name"], row["item"])
	// Output:
	// Alice laptop
}

func ExampleDataFrame_Concat() {
	jan := frame.NewDataFrameFromRecords([]map[string]any{
		{"month": "Jan", "sales": 10},
	})
	feb := frame.NewDataFrameFromRecords([]map[string]any{
		{"month": "Feb", "sales": 12},
	})
	mar := frame.NewDataFrameFromRecords([]map[string]any{
		{"month": "Mar", "sales": 14},
	})

	all, err := jan.Concat(feb, mar)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(all.Len())
	// Output:
	// 3
}

func ExampleConcatAll() {
	users := frame.NewDataFrameFromRecords([]map[string]any{
		{"name": "Alice", "age": 30},
	})
	offices := frame.NewDataFrameFromRecords([]map[string]any{
		{"name": "Bob", "city": "Tokyo"},
	})

	combined, err := frame.ConcatAll(users, offices)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%v %d\n", combined.Columns(), combined.Len())
	// Output:
	// [age city name] 2
}

func ExampleDataFrame_GroupBy() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"region": "east", "sales": 10},
		{"region": "east", "sales": 20},
		{"region": "west", "sales": 15},
	})

	grouped, err := df.GroupBy("region")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(grouped.Count().Len())
	// Output:
	// 2
}

func ExampleGroupedDataFrame_Count() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"region": "east", "sales": 10},
		{"region": "east", "sales": 20},
		{"region": "west", "sales": 15},
	})

	grouped, err := df.GroupBy("region")
	if err != nil {
		log.Fatal(err)
	}

	row := grouped.Count().ToRecords()[0]
	fmt.Printf("%s %v\n", row["region"], row["count"])
	// Output:
	// east 2
}

func ExampleGroupedDataFrame_Agg() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"region": "east", "sales": 10},
		{"region": "east", "sales": 20},
		{"region": "west", "sales": 15},
	})

	grouped, err := df.GroupBy("region")
	if err != nil {
		log.Fatal(err)
	}

	spread, err := grouped.Agg("sales", func(values []any) any {
		low, ok := values[0].(int)
		if !ok {
			log.Fatalf("sales has type %T, want int", values[0])
		}
		high := low
		for _, value := range values[1:] {
			n, ok := value.(int)
			if !ok {
				log.Fatalf("sales has type %T, want int", value)
			}
			if n < low {
				low = n
			}
			if n > high {
				high = n
			}
		}
		return high - low
	})
	if err != nil {
		log.Fatal(err)
	}

	row := spread.ToRecords()[0]
	fmt.Printf("%s %v\n", row["region"], row["agg_sales"])
	// Output:
	// east 10
}

func ExampleGroupedDataFrame_Sum() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"region": "east", "sales": 10},
		{"region": "east", "sales": 20},
		{"region": "west", "sales": 15},
	})

	grouped, err := df.GroupBy("region")
	if err != nil {
		log.Fatal(err)
	}

	sum, err := grouped.Sum("sales")
	if err != nil {
		log.Fatal(err)
	}

	row := sum.ToRecords()[0]
	fmt.Printf("%s %.0f\n", row["region"], row["sum_sales"])
	// Output:
	// east 30
}

func ExampleDataFrame_Sort() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"product": "banana", "qty": int64(1)},
		{"product": "apple", "qty": int64(3)},
	})

	sorted, err := df.Sort("qty", frame.Descending)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(sorted.ToRecords()[0]["product"])
	// Output:
	// apple
}

func ExampleDataFrame_SortBy() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"category": "fruit", "product": "banana", "qty": int64(1)},
		{"category": "fruit", "product": "apple", "qty": int64(3)},
		{"category": "vegetable", "product": "carrot", "qty": int64(2)},
	})

	sorted, err := df.SortBy(
		frame.SortOption{Column: "category", Order: frame.Ascending},
		frame.SortOption{Column: "qty", Order: frame.Descending},
	)
	if err != nil {
		log.Fatal(err)
	}

	row := sorted.ToRecords()[0]
	fmt.Printf("%s %s\n", row["category"], row["product"])
	// Output:
	// fruit apple
}

func ExampleDataFrame_DistinctBy() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"email": "alice@example.com", "team": "east"},
		{"email": "alice@example.com", "team": "west"},
		{"email": "bob@example.com", "team": "east"},
	})

	fmt.Println(df.DistinctBy("email").Len())
	// Output:
	// 2
}

func ExampleDataFrame_Head() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"name": "Alice"},
		{"name": "Bob"},
		{"name": "Cora"},
	})

	head := df.Head(2)
	fmt.Printf("%d %s\n", head.Len(), head.ToRecords()[0]["name"])
	// Output:
	// 2 Alice
}

func ExampleDataFrame_RenameColumns() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"name": "apple", "qty": 3},
	})

	renamed, err := df.RenameColumns(map[string]string{
		"name": "product",
		"qty":  "units",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(renamed.Columns())
	// Output:
	// [product units]
}

func ExampleDataFrame_FillNAByColumn() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"task": "import", "status": nil, "owner": "alice"},
		{"task": "review", "status": "done", "owner": nil},
	})

	filled := df.FillNAByColumn(map[string]any{
		"status": "pending",
		"owner":  "unassigned",
	})

	rows := filled.ToRecords()
	fmt.Printf("%s %s\n", rows[0]["status"], rows[1]["owner"])
	// Output:
	// pending unassigned
}

func ExampleDataFrame_DropNASubset() {
	df := frame.NewDataFrameFromRecords([]map[string]any{
		{"name": "Alice", "email": "alice@example.com"},
		{"name": "Bob", "email": ""},
		{"name": "Cora", "email": "cora@example.com"},
	})

	cleaned := df.DropNASubset("email")
	fmt.Println(cleaned.Len())
	// Output:
	// 2
}
