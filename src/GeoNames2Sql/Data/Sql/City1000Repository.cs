namespace GeoNames2Sql;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using NGeoNames;
using NGeoNames.Entities;

class City1000Repository
{

	private readonly string _connectionString;
	private readonly IOptions<AppSettings> _settings;

	public City1000Repository(IOptions<AppSettings> settings) {
		_connectionString = settings.Value.ConnectionString;
		_settings = settings;
	}

	public async Task SaveCity500() {
		Console.WriteLine("City 500 ready to populate admin 1 codes...");
		
		var filePath = Path.Combine(_settings.Value.DataDirectory, "cities500.txt");

		if (!File.Exists(filePath))
		{
			Console.WriteLine("Downloading City 500...");
			var downloader = GeoFileDownloader.CreateGeoFileDownloader();
			downloader.DownloadFile("cities500.zip", _settings.Value.DataDirectory);
		}
		
		var results = GeoFileReader.ReadExtendedGeoNames(filePath)
			.OrderBy(p => p.Id);
		var inserter = new City500Insert(_connectionString);
		
		foreach (var admin1Code in results) {
			await inserter.BulkInsert(admin1Code);
		}

		await inserter.Execute();
		
		Console.WriteLine();
		Console.WriteLine("City 500 added to database.");
	}


}

public class City500Insert
{

	private SqlBulkCopy _bukcCopySQL;
	private List<GeoNameDto> _geoNames;
	private readonly int _bulkSize = 300;
	private int _bulkNumber = 0;

	public City500Insert(string connectionString) {
		_bukcCopySQL = new SqlBulkCopy(connectionString);
		_bukcCopySQL.DestinationTableName = "dbo.City500";
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