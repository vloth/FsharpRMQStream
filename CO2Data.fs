module CO2Data

open FSharp.Data
open Utils

type CO2Emission =
    { CountryName: string
      CountryCode: string
      Region: string
      IndicatorName: string
      Year1990: float option
      Year1991: float option
      Year1992: float option
      Year1993: float option
      Year1994: float option
      Year1995: float option
      Year1996: float option
      Year1997: float option
      Year1998: float option
      Year1999: float option
      Year2000: float option
      Year2001: float option
      Year2002: float option
      Year2003: float option
      Year2004: float option
      Year2005: float option
      Year2006: float option
      Year2007: float option
      Year2008: float option
      Year2009: float option
      Year2010: float option
      Year2011: float option
      Year2012: float option
      Year2013: float option
      Year2014: float option
      Year2015: float option
      Year2016: float option
      Year2017: float option
      Year2018: float option
      Year2019_1: float option
      Year2019_2: float option }

[<Literal>]
let ResolutionFolder = __SOURCE_DIRECTORY__

type CO2EmissionProvider = CsvProvider<"data_provider/CO2_emission.csv", ResolutionFolder=ResolutionFolder>

let data =
    CO2EmissionProvider
        .Load(
            __SOURCE_DIRECTORY__
            + "/data_provider/CO2_emission.csv"
        )
        .Cache()

let private toCO2Emission (row: CO2EmissionProvider.Row) =
    { CountryName = row.``Country Name``
      CountryCode = row.Country_code
      Region = row.Region
      IndicatorName = row.``Indicator Name``
      Year1990 = optionalFloat row.``1990``
      Year1991 = optionalFloat row.``1991``
      Year1992 = optionalFloat row.``1992``
      Year1993 = optionalFloat row.``1993``
      Year1994 = optionalFloat row.``1994``
      Year1995 = optionalFloat row.``1995``
      Year1996 = optionalFloat row.``1996``
      Year1997 = optionalFloat row.``1997``
      Year1998 = optionalFloat row.``1998``
      Year1999 = optionalFloat row.``1999``
      Year2000 = optionalFloat row.``2000``
      Year2001 = optionalFloat row.``2001``
      Year2002 = optionalFloat row.``2002``
      Year2003 = optionalFloat row.``2003``
      Year2004 = optionalFloat row.``2004``
      Year2005 = optionalFloat row.``2005``
      Year2006 = optionalFloat row.``2006``
      Year2007 = optionalFloat row.``2007``
      Year2008 = optionalFloat row.``2008``
      Year2009 = optionalFloat row.``2009``
      Year2010 = optionalFloat row.``2010``
      Year2011 = optionalFloat row.``2011``
      Year2012 = optionalFloat row.``2012``
      Year2013 = optionalFloat row.``2013``
      Year2014 = optionalFloat row.``2014``
      Year2015 = optionalFloat row.``2015``
      Year2016 = optionalFloat row.``2016``
      Year2017 = optionalFloat row.``2017``
      Year2018 = optionalFloat row.``2018``
      Year2019_1 = optionalFloat row.``2019_1``
      Year2019_2 = optionalFloat row.``2019_2`` }

let getCO2Entries () = data.Rows |> Seq.map toCO2Emission
