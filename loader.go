package gomongofixtures

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mongodb/mongo-tools-common/bsonutil"
	"github.com/mongodb/mongo-tools-common/json"
	"go.mongodb.org/mongo-driver/mongo"
)

// Loader loads fixtures into a database.
type Loader struct {
	DB    *mongo.Database
	Paths map[string]string // Paths to fixture file.
}

// Load connects to the database and loads the fixture.
func (l *Loader) Load(ctx context.Context) error {
	for collection, path := range l.Paths {
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s for collection %s: %w", path, collection, err)
		}

		var ops []mongo.WriteModel
		d := json.NewDecoder(f)
		for {
			obj, err := d.ScanObject()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			doc, err := json.UnmarshalBsonD(obj)
			if err != nil {
				return fmt.Errorf("failed to unmarshal bsonD for collection %s: %w", collection, err)
			}

			bsonD, err := bsonutil.GetExtendedBsonD(doc)
			if err != nil {
				return fmt.Errorf("failed to get extended bsonD for collection %s: %w", collection, err)
			}

			ops = append(ops, mongo.NewInsertOneModel().SetDocument(bsonD))
		}

		if _, err = l.DB.Collection(collection).BulkWrite(ctx, ops); err != nil {
			return fmt.Errorf("failed to insert documents for collection %s: %w", collection, err)
		}
	}

	return nil
}

// Fixture describes a fixture: path to a file, database and collection the fixture
// should be loaded in.
type Fixture struct {
	Path string
}

// Load loads the given fixture into a database with the given URI using Loader.
// Each time it creates new Loader and new connection to the database.
// It uses file name in the path of the fixture as a collection name.
func Load(ctx context.Context, db *mongo.Database, root string) error {
	paths := make(map[string]string)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		collection := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		paths[collection] = path

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk through the root directory: %w", err)
	}

	l := Loader{
		Paths: paths,
		DB:    db,
	}

	return l.Load(ctx)
}
