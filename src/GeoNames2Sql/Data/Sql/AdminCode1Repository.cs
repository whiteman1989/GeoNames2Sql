namespace GeoNames2Sql;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using NGeoNames;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NGeoNames.Entities;

class AdminCode1Repository
{

	private readonly string _connectionString;
	private readonly IOptions<AppSettings> _settings;

	public AdminCode1Repository(IOptions<AppSettings> settings) {
		_connectionString = settings.Value.ConnectionString;
		_settings = settings;
	}

	public async Task SaveCAdmin1Code() {
		Console.WriteLine("Getting ready to populate admin 1 codes...");
		
		var filePath = Path.Combine(_settings.Value.DataDirectory, "admin1CodesASCII.txt");

		if (!File.Exists(filePath))
		{
			Console.WriteLine("Downloading admin 1 codes...");
			var downloader = GeoFileDownloader.CreateGeoFileDownloader();
			downloader.DownloadFile("admin1CodesASCII.txt", _settings.Value.DataDirectory);
		}
		
		var results = GeoFileReader.ReadAdmin1Codes(filePath)
			.OrderBy(p => p.GeoNameId);
		var inserter = new Admin1CodeBulkInsert(_connectionString);
		
		foreach (var admin1Code in results) {
			await inserter.BulkInsert(admin1Code);
		}

		await inserter.Execute();
		
		Console.WriteLine();
		Console.WriteLine("Admin 1 code added to database.");
	}

}

public class Admin1CodeDto
{

	public string CountryCode { get; set; }
	public string AdminCode { get; set; }
	public string Name { get; set; }
	public string NameAscii { get; set; }
	public int GeoNameId { get; set; }

}

public class Admin1CodeBulkInsert: IDisposable
{

	private SqlBulkCopy _bukcCopySQL;
	private List<Admin1CodeDto> _adminCodes;
	private readonly int _bulkSize = 300;
	private int _bulkNumber = 0;

	public Admin1CodeBulkInsert(string connectionString) {
		_bukcCopySQL = new SqlBulkCopy(connectionString);
		_bukcCopySQL.DestinationTableName = "dbo.Admin1Code";
		_adminCodes = new List<Admin1CodeDto>();
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin1CodeDto.CountryCode), "CountryCode");
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin1CodeDto.AdminCode), "AdminCode");
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin1CodeDto.Name), "Name");
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin1CodeDto.NameAscii), "NameASCII");
		_bukcCopySQL.ColumnMappings.Add(nameof(Admin1CodeDto.GeoNameId), "GeoNameId");
	}

	public async Task BulkInsert(Admin1Code admin1Code) {
		string[] codes = admin1Code.Code.Split('.');

		string countryCode = codes[0];
		string adminCode = codes[1];
		var dto = new Admin1CodeDto() {
			CountryCode = countryCode,
			AdminCode = adminCode,
			Name = admin1Code.Name,
			NameAscii = admin1Code.NameASCII,
			GeoNameId = admin1Code.GeoNameId
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