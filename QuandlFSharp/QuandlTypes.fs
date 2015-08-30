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

type DataRequest =
    {
        Database:string
        Dataset:string
        DataFormat:DataFormat
    }

// Set any expected constraint. If it's not set it will be ignored.
type Constraints =
    {
        // do not include data column names
        ExcludeColumnNames:bool option

        // get n first rows
        Truncate:int option
        
        // set a start date
        StartDate:DateTime option
        
        // set an end date
        EndDate:DateTime option
        
        // get specific column
        ColumnIndex:int option
        
        // set data frequency
        Frequency:Frequency option
        
        // set expected calculations
        Calculations:Calculations option
    }

type INoConstraintsRequest = interface end
type IRequest= interface end

type BaseUrl =
    | BaseUrl of string
    interface INoConstraintsRequest
    interface IRequest

type RegisteredUrl =
    | RegisteredUrl of string
    interface INoConstraintsRequest
    interface IRequest

type ConstrainedUrl  =
    | ConstrainedUrl of string
    interface IRequest

let quandlApiDatasetsUrl = @"https://www.quandl.com/api/v3/datasets"

// all the parameters sent to method as separate entries
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
    Http.RequestString keyUrl

// use DataRequest type

let createBaseRequest dr =
    BaseUrl(getBaseUrl dr.Database dr.Dataset dr.DataFormat)
    

let addKey key baseUrl =
    RegisteredUrl(sprintf "%A?api_key=%s" baseUrl key)
    
let addConstrains (cnstr:Constraints) (noCnstrUrl:INoConstraintsRequest) =

    // check if it's registered url if not then:
    // check if all constraints are null if no then add "?" at the end of the string
    
    // if registered add "&" at the end

    // get separate strings for each argument. Then build string by String.concat
    let exCl = if cnstr.ExcludeColumnNames.IsSome && cnstr.ExcludeColumnNames.Value then "exclude_column_names=true" else ""
    let trnc = if cnstr.Truncate.IsSome then sprintf "rows=%i" cnstr.Truncate.Value else ""
    let strt = if cnstr.StartDate.IsSome then sprintf "start_date=%s" (cnstr.StartDate.Value.ToString("yyyy-mm-dd")) else ""
    let stp = if cnstr.EndDate.IsSome then sprintf "end_date=%s" (cnstr.EndDate.Value.ToString("yyyy-mm-dd")) else ""
    let clIx = if cnstr.ColumnIndex.IsSome then sprintf "column_index=%i" cnstr.ColumnIndex.Value else ""
    let frq = 
        if cnstr.Frequency.IsSome then
            match cnstr.Frequency.Value with
            | Frequency.None -> "collapse=none"
            | Frequency.Daily -> "collapse=daily"
            | Frequency.Weekly -> "collapse=weekly"
            | Frequency.Monthly -> "collapse=monthly"
            | Frequency.Quarterly -> "collapse=quarterly"
            | Frequency.Annual -> "collapse=annual"
        else ""
    let clc = 
        if cnstr.Calculations.IsSome then
            match cnstr.Calculations.Value with
            | Calculations.Diff -> "transform=diff"
            | Calculations.RDiff -> "transform=rdiff"
            | Calculations.Cumul -> "transform=cumul"
            | Calculations.Normalize -> "transform=normalize"
        else ""
    
    let cnstrs = [|exCl;trnc;strt;stp;clIx;frq;clc|]
    let cnstrString = String.concat "&" cnstrs
    let initSep = if noCnstrUrl :? RegisteredUrl then "&" else "?"
    ConstrainedUrl(sprintf "%A%s%s" noCnstrUrl initSep cnstrString)
        
let sendQuandlRequest (req:IRequest) =
    Http.RequestString (sprintf "%A" req)
    
    //check if any has value. check if it's a request with key
    // if not then add "?" after the base url else start with "&"


    // IMPORTANT
    // The idea is to allow the user to chain functions to extend the url
    // e.g.
    //      basicUrl
    //      |> addKey "abcd"
    //      |> addConstraints myCnstr
    //      |> requestFromQuandl
    

// Class style implementation
type Loader (apiKey:string) =
    let key = apiKey
    new () = Loader("")
    member this.Get db ds df = 
        if key = "" then
            get db ds df
        else
            getRegistered db ds df apiKey



