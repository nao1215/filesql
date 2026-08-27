package prep

// This file holds the code tables the iso3166_1_* and iso4217 validators look
// a cell up in. The assignments are published facts rather than anything this
// package decides, so each table names where it was read from and when. Codes
// change rarely -- a handful a decade -- so a source comment is the whole
// maintenance story and there is no refresh machinery to go stale.

// iso3166Country is one ISO 3166-1 assignment in the three forms the standard
// publishes: the two-letter code, the three-letter code, and the three-digit
// numeric code, which carries the leading zeros the standard itself prints.
type iso3166Country struct {
	alpha2  string
	alpha3  string
	numeric string
}

// iso3166Countries is every officially assigned ISO 3166-1 code, 249 of them.
// The user-assigned range and the codes ISO has withdrawn are deliberately
// absent, so XK, which is widely used for Kosovo and is not assigned, is not
// a country code here.
//
// Source: the ISO 3166-1 list of officially assigned codes published by the
// standard's maintenance agency, read on 2026-08-27 from the machine-readable
// copy at github.com/lukes/ISO-3166-Countries-with-Regional-Codes and checked
// against the count the standard publishes.
//
//nolint:gochecknoglobals // a published code table, read-only
var iso3166Countries = [...]iso3166Country{
	{"AD", "AND", "020"},
	{"AE", "ARE", "784"},
	{"AF", "AFG", "004"},
	{"AG", "ATG", "028"},
	{"AI", "AIA", "660"},
	{"AL", "ALB", "008"},
	{"AM", "ARM", "051"},
	{"AO", "AGO", "024"},
	{"AQ", "ATA", "010"},
	{"AR", "ARG", "032"},
	{"AS", "ASM", "016"},
	{"AT", "AUT", "040"},
	{"AU", "AUS", "036"},
	{"AW", "ABW", "533"},
	{"AX", "ALA", "248"},
	{"AZ", "AZE", "031"},
	{"BA", "BIH", "070"},
	{"BB", "BRB", "052"},
	{"BD", "BGD", "050"},
	{"BE", "BEL", "056"},
	{"BF", "BFA", "854"},
	{"BG", "BGR", "100"},
	{"BH", "BHR", "048"},
	{"BI", "BDI", "108"},
	{"BJ", "BEN", "204"},
	{"BL", "BLM", "652"},
	{"BM", "BMU", "060"},
	{"BN", "BRN", "096"},
	{"BO", "BOL", "068"},
	{"BQ", "BES", "535"},
	{"BR", "BRA", "076"},
	{"BS", "BHS", "044"},
	{"BT", "BTN", "064"},
	{"BV", "BVT", "074"},
	{"BW", "BWA", "072"},
	{"BY", "BLR", "112"},
	{"BZ", "BLZ", "084"},
	{"CA", "CAN", "124"},
	{"CC", "CCK", "166"},
	{"CD", "COD", "180"},
	{"CF", "CAF", "140"},
	{"CG", "COG", "178"},
	{"CH", "CHE", "756"},
	{"CI", "CIV", "384"},
	{"CK", "COK", "184"},
	{"CL", "CHL", "152"},
	{"CM", "CMR", "120"},
	{"CN", "CHN", "156"},
	{"CO", "COL", "170"},
	{"CR", "CRI", "188"},
	{"CU", "CUB", "192"},
	{"CV", "CPV", "132"},
	{"CW", "CUW", "531"},
	{"CX", "CXR", "162"},
	{"CY", "CYP", "196"},
	{"CZ", "CZE", "203"},
	{"DE", "DEU", "276"},
	{"DJ", "DJI", "262"},
	{"DK", "DNK", "208"},
	{"DM", "DMA", "212"},
	{"DO", "DOM", "214"},
	{"DZ", "DZA", "012"},
	{"EC", "ECU", "218"},
	{"EE", "EST", "233"},
	{"EG", "EGY", "818"},
	{"EH", "ESH", "732"},
	{"ER", "ERI", "232"},
	{"ES", "ESP", "724"},
	{"ET", "ETH", "231"},
	{"FI", "FIN", "246"},
	{"FJ", "FJI", "242"},
	{"FK", "FLK", "238"},
	{"FM", "FSM", "583"},
	{"FO", "FRO", "234"},
	{"FR", "FRA", "250"},
	{"GA", "GAB", "266"},
	{"GB", "GBR", "826"},
	{"GD", "GRD", "308"},
	{"GE", "GEO", "268"},
	{"GF", "GUF", "254"},
	{"GG", "GGY", "831"},
	{"GH", "GHA", "288"},
	{"GI", "GIB", "292"},
	{"GL", "GRL", "304"},
	{"GM", "GMB", "270"},
	{"GN", "GIN", "324"},
	{"GP", "GLP", "312"},
	{"GQ", "GNQ", "226"},
	{"GR", "GRC", "300"},
	{"GS", "SGS", "239"},
	{"GT", "GTM", "320"},
	{"GU", "GUM", "316"},
	{"GW", "GNB", "624"},
	{"GY", "GUY", "328"},
	{"HK", "HKG", "344"},
	{"HM", "HMD", "334"},
	{"HN", "HND", "340"},
	{"HR", "HRV", "191"},
	{"HT", "HTI", "332"},
	{"HU", "HUN", "348"},
	{"ID", "IDN", "360"},
	{"IE", "IRL", "372"},
	{"IL", "ISR", "376"},
	{"IM", "IMN", "833"},
	{"IN", "IND", "356"},
	{"IO", "IOT", "086"},
	{"IQ", "IRQ", "368"},
	{"IR", "IRN", "364"},
	{"IS", "ISL", "352"},
	{"IT", "ITA", "380"},
	{"JE", "JEY", "832"},
	{"JM", "JAM", "388"},
	{"JO", "JOR", "400"},
	{"JP", "JPN", "392"},
	{"KE", "KEN", "404"},
	{"KG", "KGZ", "417"},
	{"KH", "KHM", "116"},
	{"KI", "KIR", "296"},
	{"KM", "COM", "174"},
	{"KN", "KNA", "659"},
	{"KP", "PRK", "408"},
	{"KR", "KOR", "410"},
	{"KW", "KWT", "414"},
	{"KY", "CYM", "136"},
	{"KZ", "KAZ", "398"},
	{"LA", "LAO", "418"},
	{"LB", "LBN", "422"},
	{"LC", "LCA", "662"},
	{"LI", "LIE", "438"},
	{"LK", "LKA", "144"},
	{"LR", "LBR", "430"},
	{"LS", "LSO", "426"},
	{"LT", "LTU", "440"},
	{"LU", "LUX", "442"},
	{"LV", "LVA", "428"},
	{"LY", "LBY", "434"},
	{"MA", "MAR", "504"},
	{"MC", "MCO", "492"},
	{"MD", "MDA", "498"},
	{"ME", "MNE", "499"},
	{"MF", "MAF", "663"},
	{"MG", "MDG", "450"},
	{"MH", "MHL", "584"},
	{"MK", "MKD", "807"},
	{"ML", "MLI", "466"},
	{"MM", "MMR", "104"},
	{"MN", "MNG", "496"},
	{"MO", "MAC", "446"},
	{"MP", "MNP", "580"},
	{"MQ", "MTQ", "474"},
	{"MR", "MRT", "478"},
	{"MS", "MSR", "500"},
	{"MT", "MLT", "470"},
	{"MU", "MUS", "480"},
	{"MV", "MDV", "462"},
	{"MW", "MWI", "454"},
	{"MX", "MEX", "484"},
	{"MY", "MYS", "458"},
	{"MZ", "MOZ", "508"},
	{"NA", "NAM", "516"},
	{"NC", "NCL", "540"},
	{"NE", "NER", "562"},
	{"NF", "NFK", "574"},
	{"NG", "NGA", "566"},
	{"NI", "NIC", "558"},
	{"NL", "NLD", "528"},
	{"NO", "NOR", "578"},
	{"NP", "NPL", "524"},
	{"NR", "NRU", "520"},
	{"NU", "NIU", "570"},
	{"NZ", "NZL", "554"},
	{"OM", "OMN", "512"},
	{"PA", "PAN", "591"},
	{"PE", "PER", "604"},
	{"PF", "PYF", "258"},
	{"PG", "PNG", "598"},
	{"PH", "PHL", "608"},
	{"PK", "PAK", "586"},
	{"PL", "POL", "616"},
	{"PM", "SPM", "666"},
	{"PN", "PCN", "612"},
	{"PR", "PRI", "630"},
	{"PS", "PSE", "275"},
	{"PT", "PRT", "620"},
	{"PW", "PLW", "585"},
	{"PY", "PRY", "600"},
	{"QA", "QAT", "634"},
	{"RE", "REU", "638"},
	{"RO", "ROU", "642"},
	{"RS", "SRB", "688"},
	{"RU", "RUS", "643"},
	{"RW", "RWA", "646"},
	{"SA", "SAU", "682"},
	{"SB", "SLB", "090"},
	{"SC", "SYC", "690"},
	{"SD", "SDN", "729"},
	{"SE", "SWE", "752"},
	{"SG", "SGP", "702"},
	{"SH", "SHN", "654"},
	{"SI", "SVN", "705"},
	{"SJ", "SJM", "744"},
	{"SK", "SVK", "703"},
	{"SL", "SLE", "694"},
	{"SM", "SMR", "674"},
	{"SN", "SEN", "686"},
	{"SO", "SOM", "706"},
	{"SR", "SUR", "740"},
	{"SS", "SSD", "728"},
	{"ST", "STP", "678"},
	{"SV", "SLV", "222"},
	{"SX", "SXM", "534"},
	{"SY", "SYR", "760"},
	{"SZ", "SWZ", "748"},
	{"TC", "TCA", "796"},
	{"TD", "TCD", "148"},
	{"TF", "ATF", "260"},
	{"TG", "TGO", "768"},
	{"TH", "THA", "764"},
	{"TJ", "TJK", "762"},
	{"TK", "TKL", "772"},
	{"TL", "TLS", "626"},
	{"TM", "TKM", "795"},
	{"TN", "TUN", "788"},
	{"TO", "TON", "776"},
	{"TR", "TUR", "792"},
	{"TT", "TTO", "780"},
	{"TV", "TUV", "798"},
	{"TW", "TWN", "158"},
	{"TZ", "TZA", "834"},
	{"UA", "UKR", "804"},
	{"UG", "UGA", "800"},
	{"UM", "UMI", "581"},
	{"US", "USA", "840"},
	{"UY", "URY", "858"},
	{"UZ", "UZB", "860"},
	{"VA", "VAT", "336"},
	{"VC", "VCT", "670"},
	{"VE", "VEN", "862"},
	{"VG", "VGB", "092"},
	{"VI", "VIR", "850"},
	{"VN", "VNM", "704"},
	{"VU", "VUT", "548"},
	{"WF", "WLF", "876"},
	{"WS", "WSM", "882"},
	{"YE", "YEM", "887"},
	{"YT", "MYT", "175"},
	{"ZA", "ZAF", "710"},
	{"ZM", "ZMB", "894"},
	{"ZW", "ZWE", "716"},
}

// iso4217Currency is one active ISO 4217 assignment in the two forms the
// standard publishes: the three-letter alphabetic code and the three-digit
// numeric code, which carries the leading zeros the standard itself prints.
type iso4217Currency struct {
	alpha   string
	numeric string
}

// iso4217Currencies is every active ISO 4217 currency, 178 of them, which
// includes the precious metals, the fund and testing codes, and XXX for a
// transaction in no currency, all of which the standard assigns. Codes the
// standard has withdrawn are absent.
//
// Source: the ISO 4217 currency code list published by SIX Financial
// Information as list-one.xml, read on 2026-08-27 from the machine-readable
// copy at github.com/datasets/currency-codes, taking the rows that carry no
// withdrawal date.
//
//nolint:gochecknoglobals // a published code table, read-only
var iso4217Currencies = [...]iso4217Currency{
	{"AED", "784"}, {"AFN", "971"}, {"ALL", "008"}, {"AMD", "051"}, {"AOA", "973"}, {"ARS", "032"},
	{"AUD", "036"}, {"AWG", "533"}, {"AZN", "944"}, {"BAM", "977"}, {"BBD", "052"}, {"BDT", "050"},
	{"BHD", "048"}, {"BIF", "108"}, {"BMD", "060"}, {"BND", "096"}, {"BOB", "068"}, {"BOV", "984"},
	{"BRL", "986"}, {"BSD", "044"}, {"BTN", "064"}, {"BWP", "072"}, {"BYN", "933"}, {"BZD", "084"},
	{"CAD", "124"}, {"CDF", "976"}, {"CHE", "947"}, {"CHF", "756"}, {"CHW", "948"}, {"CLF", "990"},
	{"CLP", "152"}, {"CNY", "156"}, {"COP", "170"}, {"COU", "970"}, {"CRC", "188"}, {"CUP", "192"},
	{"CVE", "132"}, {"CZK", "203"}, {"DJF", "262"}, {"DKK", "208"}, {"DOP", "214"}, {"DZD", "012"},
	{"EGP", "818"}, {"ERN", "232"}, {"ETB", "230"}, {"EUR", "978"}, {"FJD", "242"}, {"FKP", "238"},
	{"GBP", "826"}, {"GEL", "981"}, {"GHS", "936"}, {"GIP", "292"}, {"GMD", "270"}, {"GNF", "324"},
	{"GTQ", "320"}, {"GYD", "328"}, {"HKD", "344"}, {"HNL", "340"}, {"HTG", "332"}, {"HUF", "348"},
	{"IDR", "360"}, {"ILS", "376"}, {"INR", "356"}, {"IQD", "368"}, {"IRR", "364"}, {"ISK", "352"},
	{"JMD", "388"}, {"JOD", "400"}, {"JPY", "392"}, {"KES", "404"}, {"KGS", "417"}, {"KHR", "116"},
	{"KMF", "174"}, {"KPW", "408"}, {"KRW", "410"}, {"KWD", "414"}, {"KYD", "136"}, {"KZT", "398"},
	{"LAK", "418"}, {"LBP", "422"}, {"LKR", "144"}, {"LRD", "430"}, {"LSL", "426"}, {"LYD", "434"},
	{"MAD", "504"}, {"MDL", "498"}, {"MGA", "969"}, {"MKD", "807"}, {"MMK", "104"}, {"MNT", "496"},
	{"MOP", "446"}, {"MRU", "929"}, {"MUR", "480"}, {"MVR", "462"}, {"MWK", "454"}, {"MXN", "484"},
	{"MXV", "979"}, {"MYR", "458"}, {"MZN", "943"}, {"NAD", "516"}, {"NGN", "566"}, {"NIO", "558"},
	{"NOK", "578"}, {"NPR", "524"}, {"NZD", "554"}, {"OMR", "512"}, {"PAB", "590"}, {"PEN", "604"},
	{"PGK", "598"}, {"PHP", "608"}, {"PKR", "586"}, {"PLN", "985"}, {"PYG", "600"}, {"QAR", "634"},
	{"RON", "946"}, {"RSD", "941"}, {"RUB", "643"}, {"RWF", "646"}, {"SAR", "682"}, {"SBD", "090"},
	{"SCR", "690"}, {"SDG", "938"}, {"SEK", "752"}, {"SGD", "702"}, {"SHP", "654"}, {"SLE", "925"},
	{"SOS", "706"}, {"SRD", "968"}, {"SSP", "728"}, {"STN", "930"}, {"SVC", "222"}, {"SYP", "760"},
	{"SZL", "748"}, {"THB", "764"}, {"TJS", "972"}, {"TMT", "934"}, {"TND", "788"}, {"TOP", "776"},
	{"TRY", "949"}, {"TTD", "780"}, {"TWD", "901"}, {"TZS", "834"}, {"UAH", "980"}, {"UGX", "800"},
	{"USD", "840"}, {"USN", "997"}, {"UYI", "940"}, {"UYU", "858"}, {"UYW", "927"}, {"UZS", "860"},
	{"VED", "926"}, {"VES", "928"}, {"VND", "704"}, {"VUV", "548"}, {"WST", "882"}, {"XAD", "396"},
	{"XAF", "950"}, {"XAG", "961"}, {"XAU", "959"}, {"XBA", "955"}, {"XBB", "956"}, {"XBC", "957"},
	{"XBD", "958"}, {"XCD", "951"}, {"XCG", "532"}, {"XDR", "960"}, {"XOF", "952"}, {"XPD", "964"},
	{"XPF", "953"}, {"XPT", "962"}, {"XSU", "994"}, {"XTS", "963"}, {"XUA", "965"}, {"XXX", "999"},
	{"YER", "886"}, {"ZAR", "710"}, {"ZMW", "967"}, {"ZWG", "924"},
}

// The three ISO 3166-1 lookups and the ISO 4217 one, built once from the
// tables above so a cell is judged by a map lookup rather than a scan.
//
//nolint:gochecknoglobals // derived once from the tables above, read-only
var (
	iso3166Alpha2Set  = newCodeSet(func(c iso3166Country) string { return c.alpha2 })
	iso3166Alpha3Set  = newCodeSet(func(c iso3166Country) string { return c.alpha3 })
	iso3166NumericSet = newCodeSet(func(c iso3166Country) string { return c.numeric })
	iso4217Set        = newCurrencySet(func(c iso4217Currency) string { return c.alpha })
	iso4217NumericSet = newCurrencySet(func(c iso4217Currency) string { return c.numeric })
)

// newCodeSet collects one of the three forms of every ISO 3166-1 assignment.
func newCodeSet(form func(iso3166Country) string) map[string]struct{} {
	set := make(map[string]struct{}, len(iso3166Countries))
	for _, country := range iso3166Countries {
		set[form(country)] = struct{}{}
	}
	return set
}

// newCurrencySet collects one of the two forms of every active ISO 4217
// assignment.
func newCurrencySet(form func(iso4217Currency) string) map[string]struct{} {
	set := make(map[string]struct{}, len(iso4217Currencies))
	for _, currency := range iso4217Currencies {
		set[form(currency)] = struct{}{}
	}
	return set
}
