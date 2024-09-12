namespace GeoNames2Sql;

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NGeoNames.Entities;

public class AdvancedBulkInsert: IDisposable
{

	private SqlBulkCopy _bukcCopySQL;
	private List<GeoNameDto> _geoNames;
	private readonly int _bulkSize = 300;
	private int _bulkNumber = 0;

	public AdvancedBulkInsert(string connectionString) {
		_bukcCopySQL = new SqlBulkCopy(connectionString);
		_bukcCopySQL.DestinationTableName = "dbo.GeoNamesBulk";
		_geoNames = new List<GeoNameDto>();
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.Id), "Id");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.Name), "Name");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.NameAscii), "NameASCII");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.Latitude), "Latitude");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.Longitude), "Longitude");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.CountryCode), "CountryCode");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.FeatureClass), "FeatureClass");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.FeatureCode), "FeatureCode");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.Population), "Population");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.Elevation), "Elevation");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.Dem), "Dem");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.TimeZone), "Timezone");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.ModificationDate), "ModificationDate");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.AdminCode1), "AdminCode1");
		_bukcCopySQL.ColumnMappings.Add(nameof(GeoNameDto.AdminCode2), "AdminCode2");
		
	}

	public async Task BulkInsert(ExtendedGeoName record) {
		var dto = new GeoNameDto() {
			Id = record.Id,
			Name = record.Name,
			NameAscii = record.NameASCII,
			CountryCode = record.CountryCode,
			Latitude = record.Latitude,
			Longitude = record.Longitude,
			FeatureClass = record.FeatureClass,
			FeatureCode = record.FeatureCode,
			Population = record.Population,
			Elevation = record.Elevation,
			Dem = record.Dem,
			TimeZone = record.Timezone,
			ModificationDate = record.ModificationDate,
			AdminCode1 = record.Admincodes[0],
			AdminCode2 = record.Admincodes[1]
			
		};
		_geoNames.Add(dto);
		if (_geoNames.Count >= _bulkSize) {
			await Execute();
		}
	}

	public async Task Execute() {
		try {
			Console.WriteLine($"Insert bilk #: {_bulkNumber}");
			await _bukcCopySQL.WriteToServerAsync(_geoNames.AsDataTable());
			_geoNames.Clear();
			_bulkNumber++;
		} catch (Exception e) {
			Console.WriteLine(e);
			throw;
		}
	}

	public void Dispose() {
		((IDisposable)_bukcCopySQL)?.Dispose();
	}

}

public class GeoNameDto
{

	public int Id { get; set; }
	public string Name { get; set; }
	public string NameAscii { get; set; }
	public string CountryCode { get; set; }
	public double Latitude { get; set; }
	public double Longitude { get; set; }
	public string FeatureClass { get; set; }
	public string FeatureCode { get; set; }
	public long Population { get; set; }
	public int? Elevation { get; set; }
	public int Dem { get; set; }
	public string TimeZone { get; set; }
	public DateTime ModificationDate { get; set; }
	public string AdminCode1 { get; set; }
	public string AdminCode2 { get; set; }
		
}

public static class IEnumerableExtensions
{
	public static DataTable AsDataTable<T>(this IEnumerable<T> data)
	{
		var properties = TypeDescriptor.GetProperties(typeof(T));
		var table = new DataTable();
		foreach (PropertyDescriptor prop in properties)
			table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
		foreach (T item in data)
		{
			DataRow row = table.NewRow();
			foreach (PropertyDescriptor prop in properties)
				row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
			table.Rows.Add(row);
		}
		return table;
	}
}