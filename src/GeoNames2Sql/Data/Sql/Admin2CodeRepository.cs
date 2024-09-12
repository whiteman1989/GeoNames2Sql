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

class Admin2CodeRepository
{

	private readonly string _connectionString;
	private readonly IOptions<AppSettings> _settings;

	public Admin2CodeRepository(IOptions<AppSettings> settings) {
		_connectionString = settings.Value.ConnectionString;
		_settings = settings;
	}

	public async Task SaveCAdmin2Code() {
		Console.WriteLine("Getting ready to populate admin 2 codes...");
		
		var filePath = Path.Combine(_settings.Value.DataDirectory, "admin2Codes.txt");

		if (!File.Exists(filePath))
		{
			Console.WriteLine("Downloading admin 2 codes...");
			var downloader = GeoFileDownloader.CreateGeoFileDownloader();
			downloader.DownloadFile("admin2Codes.txt", _settings.Value.DataDirectory);
		}
		
		var results = GeoFileReader.ReadAdmin2Codes(filePath)
			.OrderBy(p => p.GeoNameId);
		var inserter = new Admin2CodeBulkInsert(_connectionString);
		
		foreach (var admin2Code in results) {
			await inserter.BulkInsert(admin2Code);
		}

		await inserter.Execute();
		
		Console.WriteLine();
		Console.WriteLine("Admin 2 code added to database.");
	}

	
}

public class Admin2CodeDto
{

	public string CountryCode { get; set; }
	public string Admin1Code { get; set; }
	public string Admin2Code { get; set; }
	public string Name { get; set; }
	public string NameAscii { get; set; }
	public int GeoNameId { get; set; }

}

public class Admin2CodeBulkInsert : IDisposable
{

	private SqlBulkCopy _bukcCopySQL;
	private List<Admin2CodeDto> _adminCodes;
	private readonly int _bulkSize = 500;
	private int _bulkNumber = 0;

	public Admin2CodeBulkInsert(string connectionString) {
		_bukcCopySQL = new SqlBulkCopy(connectionString);
		_bukcCopySQL.DestinationTableName = "dbo.Admin2Code";
		_adminCodes = new List<Admin2CodeDto>();
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin2CodeDto.CountryCode), "CountryCode");
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin2CodeDto.Admin1Code), "Admin1Code");
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin2CodeDto.Admin2Code), "Admin2Code");
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin2CodeDto.Name), "Name");
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin2CodeDto.NameAscii), "NameASCII");
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin2CodeDto.GeoNameId), "GeoNameId");
	}

	public async Task BulkInsert(Admin2Code adminCode) {
		string[] codes = adminCode.Code.Split('.');

		string countryCode = codes[0];
		string admin1Code = codes[1];
		string admin2Code = codes[2];
		var dto = new Admin2CodeDto() {
			CountryCode = countryCode,
			Admin1Code = admin1Code,
			Admin2Code = admin2Code,
			Name = adminCode.Name,
			NameAscii = adminCode.NameASCII,
			GeoNameId = adminCode.GeoNameId
		};
		_adminCodes.Add(dto);
		if (_adminCodes.Count >= _bulkSize) {
			await Execute();
		}
	}

	public async Task Execute() {
		try {
			Console.WriteLine($"Insert bulk #: {_bulkNumber}");
			await _bukcCopySQL.WriteToServerAsync(_adminCodes.AsDataTable());
			_adminCodes.Clear();
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
