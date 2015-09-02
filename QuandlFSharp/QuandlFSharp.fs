module QuandlFSharp

// TODO:
// - Add documentation - check if there is any plugin that will support standrd .Net documentation
// - Check if works for forex data

open System
open FSharp.Core
open FSharp.Data

let QUANDL_API_DATASET_URL = @"https://www.quandl.com/api/v3/datasets"

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

type INoConstraintsRequest = interface end
type IRequest= interface end

type DataRequest =
    {
        Database:string
        Dataset:string
    }

type DataFormatting = 
    {
        DataFormat:DataFormat
        SortOrder:SortOrder option
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

type BaseUrl =
    | Url of string
    interface INoConstraintsRequest
    interface IRequest

type RegisteredUrl =
    | Url of string
    interface INoConstraintsRequest
    interface IRequest

type ConstrainedUrl  =
    | Url of string
    interface IRequest

type MetadataUrl =
    | Url of string
    interface IRequest

let createDataRequest dr df =
    let getBaseUrl database dataset dataFormat =
        let format = 
            match dataFormat with
            // TODO:
            // the "data" part may be unnecessary/harmful
            // replace it with dataset.format, e.g. AAPL.csv
            | DataFormat.CSV -> "data.csv"
            | DataFormat.XML -> "data.xml"
            | DataFormat.JSON -> "data.json"
        String.concat "/" [|QUANDL_API_DATASET_URL;database;dataset;format|]
    if df.SortOrder.IsSome then
        let urlNoOrdering = getBaseUrl dr.Database dr.Dataset df.DataFormat
        let urlOrdered = sprintf "%s?order=%s" urlNoOrdering (if df.SortOrder.Value = SortOrder.Ascending then "asc" else "desc")
        BaseUrl.Url(urlOrdered)
    else
        BaseUrl.Url(getBaseUrl dr.Database dr.Dataset df.DataFormat)
    
let createMetadataRequest dr df =
    let getBaseUrl database dataset dataFormat =
        let format = 
            match dataFormat with
            | DataFormat.CSV -> "metadata.csv"
            | DataFormat.XML -> "metadata.xml"
            | DataFormat.JSON -> "metadata.json"
        String.concat "/" [|QUANDL_API_DATASET_URL;database;dataset;format|]
    MetadataUrl.Url(getBaseUrl dr.Database dr.Dataset df.DataFormat)

let addKey key baseUrl =
    // Deconstruct into a string.
    let (BaseUrl.Url baseUrl') = baseUrl
    let connector = if baseUrl'.Contains("?") then "&" else "?"
    RegisteredUrl.Url(sprintf "%s%sapi_key=%s" baseUrl' connector key)
    
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
    
    let cnstrs = String.concat "&" [|exCl;trnc;strt;stp;clIx;frq;clc|]
    let noCnstrUrl' =
        match noCnstrUrl with
        | :? BaseUrl as baseUrl -> 
            let (BaseUrl.Url baseUrl') = baseUrl
            baseUrl'
        | :? RegisteredUrl as regUrl -> 
            let (RegisteredUrl.Url regUrl') = regUrl
            regUrl'
        | _ -> failwith "Unsupported URL type [%A]" <| noCnstrUrl.GetType()
    let connector = if noCnstrUrl'.Contains("?") then "&" else "?"
    ConstrainedUrl.Url(sprintf "%s%s%s" noCnstrUrl' connector cnstrs)

let sendDataRequest (req:IRequest) =
    let req' = 
        match req with
        | :? BaseUrl as baseUrl ->
            let (BaseUrl.Url baseUrl') = baseUrl
            baseUrl'
        | :? RegisteredUrl as regUrl ->
            let (RegisteredUrl.Url regUrl') = regUrl
            regUrl'
        | :? ConstrainedUrl as cnrUrl ->
            let (ConstrainedUrl.Url cnrUrl') = cnrUrl
            cnrUrl'
        | :? MetadataUrl as metaUrl ->
            let (MetadataUrl.Url metaUrl') = metaUrl
            metaUrl'
        | _ -> failwith "Unsupported URL type [%A]" <| req.GetType()
    Http.RequestString (sprintf "%s" req')


    
//check if any has value. check if it's a request with key
// if not then add "?" after the base url else start with "&"

// There are APIs for different type of data: stock , futures, earnings
// housing, etc. check if it's possible to e.g. have a function
// like toStock that adds appropriate string to the instrument name.


// TODO change the repository of this project to the repository of the whole solution
// Or move it to separate project and create a separate repository for it.