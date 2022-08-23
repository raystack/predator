package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableSpec(t *testing.T) {
	t.Run("TableID", func(t *testing.T) {
		tableSpec := &TableSpec{
			ProjectName: "a",
			DatasetName: "b",
			TableName:   "c",
		}

		id := tableSpec.TableID()
		assert.Equal(t, "a.b.c", id)
	})
	t.Run("FieldsFlatten", func(t *testing.T) {
		address := &FieldSpec{
			Name:      "address",
			FieldType: FieldTypeRecord,
			Mode:      ModeRepeated,
			Parent:    nil,
			Level:     1,
		}

		city := &FieldSpec{
			Name:      "city",
			FieldType: FieldTypeString,
			Mode:      ModeNullable,
			Parent:    address,
			Level:     2,
		}
		country := &FieldSpec{
			Name:      "country",
			FieldType: FieldTypeString,
			Mode:      ModeNullable,
			Parent:    address,
			Level:     2,
		}

		address.Fields = []*FieldSpec{city, country}

		tableSpec := &TableSpec{
			ProjectName:    "abc",
			DatasetName:    "def",
			TableName:      "ghi",
			PartitionField: "",
			Fields: []*FieldSpec{
				address,
			},
		}

		expected := []*FieldSpec{
			address,
			city,
			country,
		}

		flatten := tableSpec.FieldsFlatten()
		assert.Equal(t, expected, flatten)
	})
	t.Run("ID", func(t *testing.T) {
		address := &FieldSpec{
			Name:      "address",
			FieldType: FieldTypeRecord,
			Mode:      ModeRepeated,
			Parent:    nil,
			Level:     1,
		}

		coordinate := &FieldSpec{
			Name:      "coordinate",
			FieldType: FieldTypeRecord,
			Mode:      ModeNullable,
			Parent:    address,
			Level:     2,
		}

		lat := &FieldSpec{
			Name:      "latitude",
			FieldType: FieldTypeNumeric,
			Mode:      ModeNullable,
			Parent:    coordinate,
			Level:     3,
		}

		long := &FieldSpec{
			Name:      "longitude",
			FieldType: FieldTypeNumeric,
			Mode:      ModeNullable,
			Parent:    coordinate,
			Level:     3,
		}

		coordinate.Fields = []*FieldSpec{lat, long}

		city := &FieldSpec{
			Name:      "city",
			FieldType: FieldTypeString,
			Mode:      ModeNullable,
			Parent:    address,
			Level:     2,
		}

		country := &FieldSpec{
			Name:      "country",
			FieldType: FieldTypeString,
			Mode:      ModeNullable,
			Parent:    address,
			Level:     2,
		}

		address.Fields = []*FieldSpec{city, country}

		assert.Equal(t, "address.country", country.ID())
		assert.Equal(t, "address.coordinate.latitude", lat.ID())
		assert.Equal(t, "address.coordinate.longitude", long.ID())
	})
	t.Run("GetFieldSpecByID", func(t *testing.T) {
		address := &FieldSpec{
			Name:      "address",
			FieldType: FieldTypeRecord,
			Mode:      ModeRepeated,
			Parent:    nil,
			Level:     1,
		}

		city := &FieldSpec{
			Name:      "city",
			FieldType: FieldTypeString,
			Mode:      ModeNullable,
			Parent:    address,
			Level:     2,
		}
		country := &FieldSpec{
			Name:      "country",
			FieldType: FieldTypeString,
			Mode:      ModeNullable,
			Parent:    address,
			Level:     2,
		}
		address.Fields = []*FieldSpec{city, country}

		tableSpec := &TableSpec{
			ProjectName:    "abc",
			DatasetName:    "def",
			TableName:      "ghi",
			PartitionField: "",
			Fields: []*FieldSpec{
				address,
			},
		}

		t.Run("should return FieldSpec of first level field", func(t *testing.T) {
			fieldSpec, _ := tableSpec.GetFieldSpecByID("address")
			assert.Equal(t, address, fieldSpec)
		})
		t.Run("should return FieldSpec of n level field", func(t *testing.T) {
			fieldSpec, _ := tableSpec.GetFieldSpecByID("address.city")
			assert.Equal(t, city, fieldSpec)
		})
		t.Run("should return ErrFieldSpecNotFound when no field spec for fieldID", func(t *testing.T) {
			_, err := tableSpec.GetFieldSpecByID("address.city.zipcode")
			assert.Equal(t, ErrFieldSpecNotFound, err)
		})
	})
}

func TestFieldSpec(t *testing.T) {
	t.Run("ID", func(t *testing.T) {
		address := &FieldSpec{
			Name:      "address",
			FieldType: FieldTypeRecord,
			Mode:      ModeRepeated,
			Parent:    nil,
			Level:     1,
		}

		city := &FieldSpec{
			Name:      "city",
			FieldType: FieldTypeString,
			Mode:      ModeNullable,
			Parent:    address,
			Level:     2,
		}

		id := city.ID()

		assert.Equal(t, "address.city", id)
	})

	t.Run("FromRootPath", func(t *testing.T) {
		address := &FieldSpec{
			Name:      "address",
			FieldType: FieldTypeRecord,
			Mode:      ModeRepeated,
			Parent:    nil,
			Level:     1,
		}

		city := &FieldSpec{
			Name:      "city",
			FieldType: FieldTypeRecord,
			Mode:      ModeNullable,
			Parent:    address,
			Level:     2,
		}

		zipCode := &FieldSpec{
			Name:      "zipcode",
			FieldType: FieldTypeString,
			Mode:      ModeNullable,
			Parent:    city,
			Level:     2,
		}

		expected := []*FieldSpec{
			address, city,
		}

		lineage := zipCode.FromRootPath()

		assert.Equal(t, expected, lineage)
	})

}
