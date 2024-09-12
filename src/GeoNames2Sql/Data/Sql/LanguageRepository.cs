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

class LanguageRepository
{

	private readonly string _connectionString;
	private readonly IOptions<AppSettings> _settings;

	public LanguageRepository(IOptions<AppSettings> settings) {
		_connectionString = settings.Value.ConnectionString;
		_settings = settings;
	}

	public async Task SaveLanguageCode() {
		Console.WriteLine("Getting ready to populate language codes...");
		
		var filePath = Path.Combine(_settings.Value.DataDirectory, "iso-languagecodes.txt");

		if (!File.Exists(filePath))
		{
			Console.WriteLine("Downloading language codes...");
			var downloader = GeoFileDownloader.CreateGeoFileDownloader();
			downloader.DownloadFile("iso-languagecodes.txt", _settings.Value.DataDirectory);
		}
		
		var results = GeoFileReader.ReadISOLanguageCodes(filePath)
			.OrderBy(p => p.LanguageName);
		var inserter = new LanguageBulkInsert(_connectionString);
		
		foreach (var language in results) {
			await inserter.BulkInsert(language);
		}

		await inserter.Execute();
		
		Console.WriteLine();
		Console.WriteLine("Language codes added to database.");
	}

}

public class LanguageDto
{

	public string CodeIso6391 { get; set; }
	public string CodeIso6392 { get; set; }
	public string CodeIso6393 { get; set; }
	public string Name { get; set; }

}

public class LanguageBulkInsert : IDisposable
{

	private SqlBulkCopy _bukcCopySQL;
	private readonly List<LanguageDto> _languages;
	private readonly int _bulkSize = 500;
	private int _bulkNumber = 0;

	public LanguageBulkInsert(string connectionString) {
		_bukcCopySQL = new SqlBulkCopy(connectionString);
		_bukcCopySQL.DestinationTableName = "dbo.Language";
		_languages = new List<LanguageDto>();
		_bukcCopySQL.ColumnMappings.Add(nameof(LanguageDto.CodeIso6391), "CodeISO639_1");
		_bukcCopySQL.ColumnMappings.Add(nameof(LanguageDto.CodeIso6392), "CodeISO639_2");
		_bukcCopySQL.ColumnMappings.Add(nameof(LanguageDto.CodeIso6393), "CodeISO639_3");
		_bukcCopySQL.ColumnMappings.Add(nameof(LanguageDto.Name), "Name");

	}

	public async Task BulkInsert(ISOLanguageCode languageCode) {
		var dto = new LanguageDto() {
			CodeIso6391 = languageCode.ISO_639_1,
			CodeIso6392 = languageCode.ISO_639_2,
			CodeIso6393 = languageCode.ISO_639_3,
			Name = languageCode.LanguageName
		};
		_languages.Add(dto);
		if (_languages.Count >= _bulkSize) {
			await Execute();
		}
	}

	public async Task Execute() {
		try {
			Console.WriteLine($"Insert bulk #: {_bulkNumber}");
			await _bukcCopySQL.WriteToServerAsync(_languages.AsDataTable());
			_languages.Clear();
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