module QuandlTypes

open System
open FSharp.Core
open FSharp.Data

type Frequency =
    | None
    | Daily
    | Weekly
    | Monthly
    | Quarterly
    | Annual

type DataFormat =
    | CSV
    | JSON
    | XML

type SortOrder =
    | Ascending
    | Descending

type Calculations =
    | Diff
    | RDiff
    | Cumul
    | Normalize

let quandlApiDatasetsUrl = @"https://www.quandl.com/api/v3/datasets"

let getBaseUrl database dataset dataFormat =
    let format = 
        match dataFormat with
        | DataFormat.CSV -> "data.csv"
        | DataFormat.XML -> "data.xml"
        | DataFormat.JSON -> "data.json"
            
    String.concat "/" [|quandlApiDatasetsUrl;database;dataset;format|]

let get database dataset dataFormat =
    Http.RequestString(getBaseUrl database dataset dataFormat)

let getRegistered database dataset dataFormat apiKey =
    let baseUrl = getBaseUrl database dataset dataFormat
    let keyUrl = sprintf "%s?api_key=%s" baseUrl apiKey
    Http.RequestString(keyUrl)

type Loader (apiKey:string) =
    let key = apiKey
    new () = Loader("")
    member this.Get db ds df = 
        if key = "" then
            get db ds df
        else
            getRegistered db ds df apiKey



