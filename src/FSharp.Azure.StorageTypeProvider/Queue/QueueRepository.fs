module internal FSharp.Azure.StorageTypeProvider.Queue.QueueRepository

open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Queue
open System
open Segment

let internal getQueueClient connectionString = CloudStorageAccount.Parse(connectionString).CreateCloudQueueClient()

let getQueues connectionString = 
    let client = getQueueClient(connectionString)
    getAllSegments
        (fun token -> client.ListQueuesSegmentedAsync(token))
        (fun queueResultSegment -> queueResultSegment.ContinuationToken)
        (fun queueResultSegment -> queueResultSegment.Results)
        null
    |> Async.map (Seq.map (fun q -> q.Name) >> Seq.toList)

let getQueueRef name = getQueueClient >> (fun q -> q.GetQueueReference name)

let peekMessages connectionString name messageCount = 
    let queueRef = getQueueRef name connectionString
    queueRef.PeekMessagesAsync(messageCount) |> Async.AwaitTask

let generateSas start duration queuePermissions (queue:CloudQueue) =
    let policy = SharedAccessQueuePolicy(Permissions = queuePermissions,
                                         SharedAccessStartTime = (start |> Option.map(fun start -> DateTimeOffset start) |> Option.toNullable),
                                         SharedAccessExpiryTime = Nullable(DateTimeOffset.UtcNow.Add duration))
    queue.GetSharedAccessSignature(policy, null)