///Contains reusable helper functions for accessing blobs
module internal FSharp.Azure.StorageTypeProvider.Blob.BlobRepository

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
open System
open System.IO
open Segment

type ContainerItem = 
    | Folder of path : string * name : string * contents : Async<ContainerItem array>
    | Blob of path : string * name : string * blobType : BlobType * length : int64 option

type LightweightContainer = 
    { Name : string
      Contents : Async<ContainerItem array> }

let getBlobClient connection = CloudStorageAccount.Parse(connection).CreateCloudBlobClient()
let getContainerRef(connection, container) = (getBlobClient connection).GetContainerReference(container)
let getBlockBlobRef (connection, container, file) = getContainerRef(connection, container).GetBlockBlobReference(file)
let getPageBlobRef (connection, container, file) = getContainerRef(connection, container).GetPageBlobReference(file)

let private getItemName (item : string) (parent : CloudBlobDirectory) = 
    item, 
    match parent with
    | null -> item
    | parent -> item.Substring(parent.Prefix.Length)

let rec private getContainerStructure wildcard (container : CloudBlobContainer) = async {
    let! blobs =
        getAllSegments
            (fun token -> container.ListBlobsSegmentedAsync(wildcard, token))
            (fun blobResultSegment -> blobResultSegment.ContinuationToken)
            (fun blobResultSegment -> blobResultSegment.Results)
            null

    let result =
        blobs
        |> Seq.distinctBy (fun b -> b.Uri.AbsoluteUri)
        |> Seq.choose (function
           | :? CloudBlobDirectory as directory -> 
               let path, name = getItemName directory.Prefix directory.Parent
               Some(Folder(path, name, (getContainerStructure directory.Prefix container)))
           | :? ICloudBlob as blob ->
               let path, name = getItemName blob.Name blob.Parent
               Some(Blob(path, name, blob.BlobType, Some blob.Properties.Length))
           | _ -> None)
        |> Seq.toArray
    return result
}

let internal listBlobsInternal (container : CloudBlobContainer) prefix includeSubDirs =
    let options = new BlobRequestOptions()
    let context = new OperationContext()
    let maxResults = new Nullable<int>()
    getAllSegments
        (fun token -> container.ListBlobsSegmentedAsync(prefix, includeSubDirs, BlobListingDetails.All, maxResults, token, options, context))
        (fun blobSegmentResult -> blobSegmentResult.ContinuationToken)
        (fun blobSegmentResult -> blobSegmentResult.Results)
        null    

let listBlobs prefix incSubDirs (container:CloudBlobContainer)  = 
    listBlobsInternal container prefix incSubDirs
    |> Async.map
        (Seq.choose(function
            | :? ICloudBlob as blob -> 
                let path, name = getItemName blob.Name blob.Parent
                Some(Blob(path, name, blob.Properties.BlobType, Some blob.Properties.Length))
            | _ -> None))    //can safely ignore folder types as we have a flat structure if & only if we want to include items from sub directories

let getBlobStorageAccountManifest connection = 
    let client = getBlobClient connection
    let buildLightWeightContainer (container : CloudBlobContainer) =
        { 
            Name = container.Name
            Contents = getContainerStructure null container
        }
    async {
        let! containers =
            getAllSegments
                (fun token -> client.ListContainersSegmentedAsync(token))
                (fun containerSegmentResult -> containerSegmentResult.ContinuationToken)
                (fun containerSegmentResult -> containerSegmentResult.Results)
                null 
             
        let result =             
            containers
            |> Seq.map buildLightWeightContainer
        return result}
    

let downloadFolder (connectionDetails, path) =
    let downloadFile (blobRef:ICloudBlob) destination =
        let targetDirectory = Path.GetDirectoryName(destination)
        if not (Directory.Exists targetDirectory) then Directory.CreateDirectory targetDirectory |> ignore
        blobRef.DownloadToFileAsync(destination, FileMode.Create) |> Async.AwaitTask

    let connection, container, folderPath = connectionDetails
    let containerRef = (getBlobClient connection).GetContainerReference(container)

    async {
        let! containerItems = listBlobsInternal containerRef folderPath true
        do!
            containerItems
            |> Seq.choose (function
                | :? ICloudBlob as b -> Some b
                | _ -> None)
            |> Seq.map (fun blob -> 
                let targetName = 
                    match folderPath with
                    | folderPath when String.IsNullOrEmpty folderPath -> blob.Name
                    | _ -> blob.Name.Replace(folderPath, String.Empty)
                downloadFile blob (Path.Combine(path, targetName)))
            |> Async.Parallel
            |> Async.Ignore}