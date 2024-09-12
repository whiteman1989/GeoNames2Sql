namespace GeoNames2Sql;

using System;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NGeoNames.Entities;

public class BulkInserter: IDisposable
{

	private readonly SqlConnection _sqlConnection;
	private readonly int _chunkSize = 250;
	private int _currentSize = 0;
	private readonly StringBuilder _commandBuilder;

	public int _bulkNumber;

	public BulkInserter(string connectionString) {
		_sqlConnection = new SqlConnection(connectionString);
		_commandBuilder = new StringBuilder("");
		_bulkNumber = 0;
		_sqlConnection.Open();
	}

	public async Task BulkInsert(ExtendedGeoName record) {
		var adminCode2 = string.IsNullOrEmpty(record.Admincodes[1]) ? $"'{record.Admincodes[1]}'" : "NULL";
		var name = $" STRING_ESCAPE('{record.Name.Replace("'", "''")}', 'json')";
		var nameAscii = $" STRING_ESCAPE('{record.NameASCII.Replace("'", "''")}', 'json')";
		var timeZone = string.IsNullOrEmpty(record.Timezone) ? $"STRING_ESCAPE('{record.Timezone}', 'json')" : "NULL";
		var updateDate = record.ModificationDate.ToString("yyyy-MM-dd HH:mm:ss");
		var cmnd =
			@$"MERGE INTO GeoNames AS t
                    USING (VALUES ({record.Id})) AS s(Id) ON (s.Id = t.Id)
                    WHEN MATCHED AND (CAST('{updateDate}' AS DATE) > t.ModificationDate) THEN
                        UPDATE SET t.Name = {name},
							t.NameASCII = {nameAscii}, t.Latitude = {record.Latitude.ToString("G", CultureInfo.InvariantCulture)}, t.Longitude = {record.Longitude.ToString("G", CultureInfo.InvariantCulture)},
                            t.FeatureClass = '{record.FeatureClass}', t.FeatureCode = '{record.FeatureCode}', t.CountryCode = '{record.CountryCode}',
                            t.Population = {record.Population}, t.Elevation = {record.Elevation?.ToString() ?? "NULL"}, t.Dem = {record.Dem}, t.Timezone = {timeZone},
                            t.ModificationDate = '{updateDate}',
                            t.AdminCode1 = {record.Admincodes[0] ?? "NULL"}, t.AdminCode2 = {adminCode2}
                    WHEN NOT MATCHED THEN
                        INSERT (Id, Name, NameASCII, Latitude, Longitude, FeatureClass, FeatureCode, CountryCode, Population, Elevation, Dem, Timezone, ModificationDate, AdminCode1, AdminCode2) 
                        VALUES ({record.Id}, {name}, {nameAscii}, {record.Latitude.ToString("G", CultureInfo.InvariantCulture)}, {record.Longitude.ToString("G", CultureInfo.InvariantCulture)}, '{record.FeatureClass}', '{record.FeatureCode}', '{record.CountryCode ?? "NULL"}', '{record.Population}', {record.Elevation?.ToString() ?? "NULL"}, {record.Dem}, {timeZone}, '{updateDate}', {record.Admincodes[0] ?? "NULL"}, {adminCode2});";
		_commandBuilder.AppendLine(cmnd);
		_currentSize++;
		if (_currentSize >= _chunkSize) { 
			
			await Execute();
		}
	}

	public async Task Execute() {
		var commandText = _commandBuilder.ToString();
		Console.WriteLine($"Inserted bulk #: {_bulkNumber}");
		if (!string.IsNullOrEmpty(commandText)) {
			var command = new SqlCommand(_commandBuilder.ToString(), _sqlConnection);
			try {
				await command.ExecuteNonQueryAsync();
				_bulkNumber++;
				_currentSize = 0;
				_commandBuilder.Clear();
			} catch (Exception e) {
				Console.WriteLine(commandText);
				Console.WriteLine(e);
				throw;
			}
		}
	}

	public void Dispose() {
		_sqlConnection?.Dispose();
	}

}