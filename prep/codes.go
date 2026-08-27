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

// iso4217Currencies is every active ISO 4217 alphabetic currency code, which
// includes the codes for the precious metals, the fund and testing codes, and
// XXX for a transaction in no currency, all of which the standard assigns.
// Codes the standard has withdrawn are absent.
//
// Source: the ISO 4217 currency code list published by SIX Financial
// Information as list-one.xml, read on 2026-08-27 from the machine-readable
// copy at github.com/datasets/currency-codes, taking the rows that carry no
// withdrawal date.
//
//nolint:gochecknoglobals // a published code table, read-only
var iso4217Currencies = [...]string{
	"AED", "AFN", "ALL", "AMD", "AOA", "ARS", "AUD", "AWG", "AZN", "BAM", "BBD", "BDT", "BHD",
	"BIF", "BMD", "BND", "BOB", "BOV", "BRL", "BSD", "BTN", "BWP", "BYN", "BZD", "CAD", "CDF",
	"CHE", "CHF", "CHW", "CLF", "CLP", "CNY", "COP", "COU", "CRC", "CUP", "CVE", "CZK", "DJF",
	"DKK", "DOP", "DZD", "EGP", "ERN", "ETB", "EUR", "FJD", "FKP", "GBP", "GEL", "GHS", "GIP",
	"GMD", "GNF", "GTQ", "GYD", "HKD", "HNL", "HTG", "HUF", "IDR", "ILS", "INR", "IQD", "IRR",
	"ISK", "JMD", "JOD", "JPY", "KES", "KGS", "KHR", "KMF", "KPW", "KRW", "KWD", "KYD", "KZT",
	"LAK", "LBP", "LKR", "LRD", "LSL", "LYD", "MAD", "MDL", "MGA", "MKD", "MMK", "MNT", "MOP",
	"MRU", "MUR", "MVR", "MWK", "MXN", "MXV", "MYR", "MZN", "NAD", "NGN", "NIO", "NOK", "NPR",
	"NZD", "OMR", "PAB", "PEN", "PGK", "PHP", "PKR", "PLN", "PYG", "QAR", "RON", "RSD", "RUB",
	"RWF", "SAR", "SBD", "SCR", "SDG", "SEK", "SGD", "SHP", "SLE", "SOS", "SRD", "SSP", "STN",
	"SVC", "SYP", "SZL", "THB", "TJS", "TMT", "TND", "TOP", "TRY", "TTD", "TWD", "TZS", "UAH",
	"UGX", "USD", "USN", "UYI", "UYU", "UYW", "UZS", "VED", "VES", "VND", "VUV", "WST", "XAD",
	"XAF", "XAG", "XAU", "XBA", "XBB", "XBC", "XBD", "XCD", "XCG", "XDR", "XOF", "XPD", "XPF",
	"XPT", "XSU", "XTS", "XUA", "XXX", "YER", "ZAR", "ZMW", "ZWG",
}

// The three ISO 3166-1 lookups and the ISO 4217 one, built once from the
// tables above so a cell is judged by a map lookup rather than a scan.
//
//nolint:gochecknoglobals // derived once from the tables above, read-only
var (
	iso3166Alpha2Set  = newCodeSet(func(c iso3166Country) string { return c.alpha2 })
	iso3166Alpha3Set  = newCodeSet(func(c iso3166Country) string { return c.alpha3 })
	iso3166NumericSet = newCodeSet(func(c iso3166Country) string { return c.numeric })
	iso4217Set        = newISO4217Set()
)

// newCodeSet collects one of the three forms of every ISO 3166-1 assignment.
func newCodeSet(form func(iso3166Country) string) map[string]struct{} {
	set := make(map[string]struct{}, len(iso3166Countries))
	for _, country := range iso3166Countries {
		set[form(country)] = struct{}{}
	}
	return set
}

// newISO4217Set collects the active currency codes.
func newISO4217Set() map[string]struct{} {
	set := make(map[string]struct{}, len(iso4217Currencies))
	for _, code := range iso4217Currencies {
		set[code] = struct{}{}
	}
	return set
}
