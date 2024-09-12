using Microsoft.Extensions.Options;
using System.Threading.Tasks;

namespace GeoNames2Sql
{
    class GeoNamesService
    {
        private readonly AlternateNamesRepository _alternateNames;
        private readonly CountryInfoRepository _countryInfo;
        private readonly GeoNamesRepository _geoNames;
        private readonly IOptions<AppSettings> _settings;
        private readonly AdminCode1Repository _adminCode1;
        private readonly Admin2CodeRepository _admin2;
        private readonly LanguageRepository _language;
        private readonly City1000Repository _city1000;

        public GeoNamesService(
            AlternateNamesRepository alternateNames,
            CountryInfoRepository countryInfo,
            GeoNamesRepository geoNames,
            AdminCode1Repository adminCode1,
            Admin2CodeRepository admin2,
            LanguageRepository language,
            City1000Repository city1000,
            IOptions<AppSettings> settings)
        {
            _alternateNames = alternateNames;
            _countryInfo = countryInfo;
            _geoNames = geoNames;
            _adminCode1 = adminCode1;
            _admin2 = admin2;
            _language = language;
            _settings = settings;
            _city1000 = city1000;
        }

        public async Task PerformOperations()
        {
            //await _geoNames.SaveGeoNames();

            //await _adminCode1.SaveCAdmin1Code();
            
            //await _admin2.SaveCAdmin2Code();

            //await _language.SaveLanguageCode();

            await _city1000.SaveCity500();

            // if (_settings.Value.GeoNames.AlternateNamesLanguages.Count > 0)
            //     await _alternateNames.SaveAlternateNames();
            //
            // if (_settings.Value.GeoNames.CountryInfo)
            //     await _countryInfo.SaveCountryInfo();
        }
    }
}
