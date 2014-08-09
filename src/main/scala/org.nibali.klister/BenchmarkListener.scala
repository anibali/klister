package org.nibali.klister

// M_LANGER: Listens to events from a spark context, aggregates and outputs
//           states in a human readable way.
import java.io._
import scala.collection.mutable._
import org.apache.spark.scheduler._
import org.apache.spark.executor._

class BenchmarkListener(outputStream: OutputStream, printTaskDetails: Boolean = true, printHostnames: Boolean = false) extends SparkListener {
  val writer = new PrintWriter(outputStream, true)
  val separatorWidth = 125

  // Min task values.
  var taskDurationMin                : Long = Long.MaxValue

  var taskDiskBytesSpilledMin        : Long = Long.MaxValue
  var taskExecutorDeserializeTimeMin : Long = Long.MaxValue
  var taskExecutorRunTimeMin         : Long = Long.MaxValue
  var taskJvmGCTimeMin               : Long = Long.MaxValue
  var taskMemoryBytesSpilledMin      : Long = Long.MaxValue
  var taskResultSerializationTimeMin : Long = Long.MaxValue
  var taskResultSizeMin              : Long = Long.MaxValue
  
  var taskFetchWaitTimeMin           : Long = Long.MaxValue
  var taskLocalBlocksFetchedMin      : Long = Long.MaxValue
  var taskRemoteBlocksFetchedMin     : Long = Long.MaxValue
  var taskRemoteBytesReadMin         : Long = Long.MaxValue
  var taskTotalBlocksFetchedMin      : Long = Long.MaxValue
  
  var taskShuffleBytesWrittenMin     : Long = Long.MaxValue
  var taskShuffleWriteTimeMin        : Long = Long.MaxValue

  // Max task values.
  var taskDurationMax                : Long = Long.MinValue

  var taskDiskBytesSpilledMax        : Long = Long.MinValue
  var taskExecutorDeserializeTimeMax : Long = Long.MinValue
  var taskExecutorRunTimeMax         : Long = Long.MinValue
  var taskJvmGCTimeMax               : Long = Long.MinValue
  var taskMemoryBytesSpilledMax      : Long = Long.MinValue
  var taskResultSerializationTimeMax : Long = Long.MinValue
  var taskResultSizeMax              : Long = Long.MinValue
  
  var taskFetchWaitTimeMax           : Long = Long.MinValue
  var taskLocalBlocksFetchedMax      : Long = Long.MinValue
  var taskRemoteBlocksFetchedMax     : Long = Long.MinValue
  var taskRemoteBytesReadMax         : Long = Long.MinValue
  var taskTotalBlocksFetchedMax      : Long = Long.MinValue
  
  var taskShuffleBytesWrittenMax     : Long = Long.MinValue
  var taskShuffleWriteTimeMax        : Long = Long.MinValue

  // Stage values.
  var stageDiskBytesSpilled        : Long = 0
  var stageExecutorDeserializeTime : Long = 0
  var stageExecutorRunTime         : Long = 0
  var stageJvmGCTime               : Long = 0
  var stageMemoryBytesSpilled      : Long = 0
  var stageResultSerializationTime : Long = 0
  var stageResultSize              : Long = 0
  
  var stageFetchWaitTime           : Long = 0
  var stageLocalBlocksFetched      : Long = 0
  var stageRemoteBlocksFetched     : Long = 0
  var stageRemoteBytesRead         : Long = 0
  var stageTotalBlocksFetched      : Long = 0
  
  var stageShuffleBytesWritten     : Long = 0
  var stageShuffleWriteTime        : Long = 0
  
  // Job values.
  var jobDiskBytesSpilled        : Long = 0
  var jobExecutorDeserializeTime : Long = 0
  var jobExecutorRunTime         : Long = 0
  var jobJvmGCTime               : Long = 0
  var jobMemoryBytesSpilled      : Long = 0
  var jobResultSerializationTime : Long = 0
  var jobResultSize              : Long = 0
  
  var jobFetchWaitTime           : Long = 0
  var jobLocalBlocksFetched      : Long = 0
  var jobRemoteBlocksFetched     : Long = 0
  var jobRemoteBytesRead         : Long = 0
  var jobTotalBlocksFetched      : Long = 0
  
  var jobShuffleBytesWritten     : Long = 0
  var jobShuffleWriteTime        : Long = 0

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    synchronized {
      writer.println("\nApplication Terminated! (Total Runtime: %.3f s)".format(applicationEnd.time / 1000.0))
    }
  }
  
  override def onJobStart(jobStart: SparkListenerJobStart) {
    synchronized {
      writer.println("\nJob #%3d...".format(jobStart.jobId))
      
      jobDiskBytesSpilled        = 0
      jobExecutorDeserializeTime = 0
      jobExecutorRunTime         = 0
      jobJvmGCTime               = 0
      jobMemoryBytesSpilled      = 0
      jobResultSerializationTime = 0
      jobResultSize              = 0
      
      jobFetchWaitTime           = 0
      jobLocalBlocksFetched      = 0
      jobRemoteBlocksFetched     = 0
      jobRemoteBytesRead         = 0
      jobTotalBlocksFetched      = 0
      
      jobShuffleBytesWritten     = 0
      jobShuffleWriteTime        = 0
    }
  }
  
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    synchronized {
      writer.println("Job #%3d = %s | MS: %5.1f MB, DS: %5.1f MB, RR: %5.1f MB, SW: %5.1f MB | FWT: %5.2f, DSER: %5.2f, RT: %6.2f, SER: %5.2f, GC: %5.2f".format(
        jobEnd.jobId,
        jobEnd.jobResult,
        jobRemoteBytesRead / 1024.0 / 1024.0,
        jobMemoryBytesSpilled / 1024.0 / 1024.0,
        jobDiskBytesSpilled / 1024.0 / 1024.0,
        jobShuffleBytesWritten / 1024.0 / 1024.0,
        jobFetchWaitTime / 1000.0,
        jobExecutorDeserializeTime / 1000.0,
        jobExecutorRunTime / 1000.0,
        jobResultSerializationTime / 1000.0,
        jobJvmGCTime / 1000.0))
    }
  }
  
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    synchronized {
      val si = stageSubmitted.stageInfo
      writer.println("Stage #%3d (%d Tasks, i.e. '%s')...".format(si.stageId, si.numTasks, si.name))
      for (i <- 1 to separatorWidth) {
        writer.print('-')
      }
      writer.println()
      
      // Stage sums
      stageDiskBytesSpilled        = 0
      stageExecutorDeserializeTime = 0
      stageExecutorRunTime         = 0
      stageJvmGCTime               = 0
      stageMemoryBytesSpilled      = 0
      stageResultSerializationTime = 0
      stageResultSize              = 0
      
      stageFetchWaitTime           = 0
      stageLocalBlocksFetched      = 0
      stageRemoteBlocksFetched     = 0
      stageRemoteBytesRead         = 0
      stageTotalBlocksFetched      = 0
      
      stageShuffleBytesWritten     = 0
      stageShuffleWriteTime        = 0

      // Min values
      taskDurationMin                = Long.MaxValue

      taskDiskBytesSpilledMin        = Long.MaxValue
      taskExecutorDeserializeTimeMin = Long.MaxValue
      taskExecutorRunTimeMin         = Long.MaxValue
      taskJvmGCTimeMin               = Long.MaxValue
      taskMemoryBytesSpilledMin      = Long.MaxValue
      taskResultSerializationTimeMin = Long.MaxValue
      taskResultSizeMin              = Long.MaxValue
  
      taskFetchWaitTimeMin           = Long.MaxValue
      taskLocalBlocksFetchedMin      = Long.MaxValue
      taskRemoteBlocksFetchedMin     = Long.MaxValue
      taskRemoteBytesReadMin         = Long.MaxValue
      taskTotalBlocksFetchedMin      = Long.MaxValue
  
      taskShuffleBytesWrittenMin     = Long.MaxValue
      taskShuffleWriteTimeMin        = Long.MaxValue

      // Max values
      taskDurationMax                = Long.MinValue

      taskDiskBytesSpilledMax        = Long.MinValue
      taskExecutorDeserializeTimeMax = Long.MinValue
      taskExecutorRunTimeMax         = Long.MinValue
      taskJvmGCTimeMax               = Long.MinValue
      taskMemoryBytesSpilledMax      = Long.MinValue
      taskResultSerializationTimeMax = Long.MinValue
      taskResultSizeMax              = Long.MinValue
  
      taskFetchWaitTimeMax           = Long.MinValue
      taskLocalBlocksFetchedMax      = Long.MinValue
      taskRemoteBlocksFetchedMax     = Long.MinValue
      taskRemoteBytesReadMax         = Long.MinValue
      taskTotalBlocksFetchedMax      = Long.MinValue
  
      taskShuffleBytesWrittenMax     = Long.MinValue
      taskShuffleWriteTimeMax        = Long.MinValue
    }
  }
  
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    synchronized {
      for (i <- 1 to separatorWidth) {
        writer.print('-')
      }
      writer.println()

      val si = stageCompleted.stageInfo
      val duration : Long = if (si.submissionTime.isEmpty || si.completionTime.isEmpty) 0 else si.completionTime.get - si.submissionTime.get
      writer.println("Stage #%3d = %s, %6.2f | %6.1f kB (RR: %5.1f MB, SW: %5.1f MB) | FWT: %5.2f, DSER: %5.2f, RT: %6.2f, SER: %5.2f, GC: %5.2f".format(
        si.stageId,
        si.failureReason.getOrElse("OK"),
        duration / 1000.0,
        stageResultSize / 1024.0,
        stageRemoteBytesRead / 1024.0 / 1024.0,
        stageShuffleBytesWritten / 1024.0 / 1024.0,
        stageFetchWaitTime / 1000.0,
        stageExecutorDeserializeTime / 1000.0,
        stageExecutorRunTime / 1000.0,
        stageResultSerializationTime / 1000.0,
        stageJvmGCTime / 1000.0))
      writer.println("  Task Max.      %6.2f | %6.1f kB (RR: %5.1f MB, SW: %5.1f MB) | FWT: %5.2f, DSER: %5.2f, RT: %6.2f, SER: %5.2f, GC: %5.2f".format(
        taskDurationMax / 1000.0,
        taskResultSizeMax / 1024.0,
        taskRemoteBytesReadMax / 1024.0 / 1024.0,
        taskShuffleBytesWrittenMax / 1024.0 / 1024.0,
        taskFetchWaitTimeMax / 1000.0,
        taskExecutorDeserializeTimeMax / 1000.0,
        taskExecutorRunTimeMax / 1000.0,
        taskResultSerializationTimeMax / 1000.0,
        taskJvmGCTimeMax / 1000.0
      ))
      writer.println("  Task Min.      %6.2f | %6.1f kB (RR: %5.1f MB, SW: %5.1f MB) | FWT: %5.2f, DSER: %5.2f, RT: %6.2f, SER: %5.2f, GC: %5.2f".format(
        taskDurationMin / 1000.0,
        taskResultSizeMin / 1024.0,
        taskRemoteBytesReadMin / 1024.0 / 1024.0,
        taskShuffleBytesWrittenMin / 1024.0 / 1024.0,
        taskFetchWaitTimeMin / 1000.0,
        taskExecutorDeserializeTimeMin / 1000.0,
        taskExecutorRunTimeMin / 1000.0,
        taskResultSerializationTimeMin / 1000.0,
        taskJvmGCTimeMin / 1000.0
      ))
      writer.println("  Task MinMaxFac %6.1f | %6.1f    (RR: %5.1f   , SW: %5.1f   ) | FWT: %5.1f, DSER: %5.1f, RT: %6.1f, SER: %5.1f, GC: %5.1f".format(
        if (taskDurationMin                != 0) taskDurationMax                 / taskDurationMin.toDouble                else Double.NaN,
        if (taskResultSizeMin              != 0) taskResultSizeMax               / taskResultSizeMin.toDouble              else Double.NaN,
        if (taskRemoteBytesReadMin         != 0) taskRemoteBytesReadMax          / taskRemoteBytesReadMin.toDouble         else Double.NaN,
        if (taskShuffleBytesWrittenMin     != 0) taskShuffleBytesWrittenMax      / taskShuffleBytesWrittenMin.toDouble     else Double.NaN,
        if (taskFetchWaitTimeMin           != 0) taskFetchWaitTimeMax            / taskFetchWaitTimeMin.toDouble           else Double.NaN,
        if (taskExecutorDeserializeTimeMin != 0) taskExecutorDeserializeTimeMax  / taskExecutorDeserializeTimeMin.toDouble else Double.NaN,
        if (taskExecutorRunTimeMin         != 0) taskExecutorRunTimeMax          / taskExecutorRunTimeMin.toDouble         else Double.NaN,
        if (taskResultSerializationTimeMin != 0) taskResultSerializationTimeMax  / taskResultSerializationTimeMin.toDouble else Double.NaN,
        if (taskJvmGCTimeMin               != 0) taskJvmGCTimeMax                / taskJvmGCTimeMin.toDouble               else Double.NaN
      ))
      writer.println("  Task Avg.      %6.2f | %6.1f kB (RR: %5.1f MB, SW: %5.1f MB) | FWT: %5.2f, DSER: %5.2f, RT: %6.2f, SER: %5.2f, GC: %5.2f".format(
        duration / 1000.0 / si.numTasks,
        stageResultSize / 1024.0 / si.numTasks,
        stageRemoteBytesRead / 1024.0 / 1024.0 / si.numTasks,
        stageShuffleBytesWritten / 1024.0 / 1024.0 / si.numTasks,
        stageFetchWaitTime / 1000.0 / si.numTasks,
        stageExecutorDeserializeTime / 1000.0 / si.numTasks,
        stageExecutorRunTime / 1000.0 / si.numTasks,
        stageResultSerializationTime / 1000.0 / si.numTasks,
        stageJvmGCTime / 1000.0 / si.numTasks
      ))
        
      jobDiskBytesSpilled        += stageDiskBytesSpilled
      jobExecutorDeserializeTime += stageExecutorDeserializeTime
      jobExecutorRunTime         += stageExecutorRunTime
      jobJvmGCTime               += stageJvmGCTime
      jobMemoryBytesSpilled      += stageMemoryBytesSpilled
      jobResultSerializationTime += stageResultSerializationTime
      jobResultSize              += stageResultSize
        
      jobFetchWaitTime           += stageFetchWaitTime
      jobLocalBlocksFetched      += stageLocalBlocksFetched
      jobRemoteBlocksFetched     += stageRemoteBlocksFetched
      jobRemoteBytesRead         += stageRemoteBytesRead
      jobTotalBlocksFetched      += stageTotalBlocksFetched
        
      jobShuffleBytesWritten     += stageShuffleBytesWritten
      jobShuffleWriteTime        += stageShuffleWriteTime
    }
  }
  
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    synchronized {
      val ti = taskEnd.taskInfo
      val tm = taskEnd.taskMetrics
      val srm = tm.shuffleReadMetrics.getOrElse(new ShuffleReadMetrics)
      val swm = tm.shuffleWriteMetrics.getOrElse(new ShuffleWriteMetrics)
      if (printTaskDetails) {
        writer.println("Task #%3d = %s, %6.2f | %6.1f kB (RR: %5.1f MB, SW: %5.1f MB) | FWT: %5.2f, DSER: %5.2f, RT: %6.2f, SER: %5.2f, GC: %5.2f%s".format(
          ti.taskId,
          if (ti.failed) "ERROR" else "OK",
          ti.duration / 1000.0,
          tm.resultSize / 1024.0,
          srm.remoteBytesRead / 1024.0 / 1024.0,
          swm.shuffleBytesWritten / 1024.0 / 1024.0,
          srm.fetchWaitTime / 1000.0,
          tm.executorDeserializeTime / 1000.0,
          tm.executorRunTime / 1000.0,
          tm.resultSerializationTime / 1000.0,
          tm.jvmGCTime / 1000.0,
          if  (printHostnames) " @ %s".format(tm.hostname) else ""))
      }

      // Stage sums
      stageDiskBytesSpilled        += tm.diskBytesSpilled
      stageExecutorDeserializeTime += tm.executorDeserializeTime
      stageExecutorRunTime         += tm.executorRunTime
      stageJvmGCTime               += tm.jvmGCTime
      stageMemoryBytesSpilled      += tm.memoryBytesSpilled
      stageResultSerializationTime += tm.resultSerializationTime
      stageResultSize              += tm.resultSize
      
      stageFetchWaitTime           += srm.fetchWaitTime
      stageLocalBlocksFetched      += srm.localBlocksFetched
      stageRemoteBlocksFetched     += srm.remoteBlocksFetched
      stageRemoteBytesRead         += srm.remoteBytesRead
      stageTotalBlocksFetched      += srm.totalBlocksFetched
      
      stageShuffleBytesWritten     += swm.shuffleBytesWritten
      stageShuffleWriteTime        += swm.shuffleWriteTime

      // Min values
      taskDurationMin                = Math.min(taskDurationMin,                ti.duration               )

      taskDiskBytesSpilledMin        = Math.min(taskDiskBytesSpilledMin,        tm.diskBytesSpilled       )
      taskExecutorDeserializeTimeMin = Math.min(taskExecutorDeserializeTimeMin, tm.executorDeserializeTime)
      taskExecutorRunTimeMin         = Math.min(taskExecutorRunTimeMin,         tm.executorRunTime        )
      taskJvmGCTimeMin               = Math.min(taskJvmGCTimeMin,               tm.jvmGCTime              )
      taskMemoryBytesSpilledMin      = Math.min(taskMemoryBytesSpilledMin,      tm.memoryBytesSpilled     )
      taskResultSerializationTimeMin = Math.min(taskResultSerializationTimeMin, tm.resultSerializationTime)
      taskResultSizeMin              = Math.min(taskResultSizeMin,              tm.resultSize             )

      taskFetchWaitTimeMin       = Math.min(taskFetchWaitTimeMin,       srm.fetchWaitTime      )
      taskLocalBlocksFetchedMin  = Math.min(taskLocalBlocksFetchedMin,  srm.localBlocksFetched )
      taskRemoteBlocksFetchedMin = Math.min(taskRemoteBlocksFetchedMin, srm.remoteBlocksFetched)
      taskRemoteBytesReadMin     = Math.min(taskRemoteBytesReadMin,     srm.remoteBytesRead    )
      taskTotalBlocksFetchedMin  = Math.min(taskTotalBlocksFetchedMin,  srm.totalBlocksFetched )

      taskShuffleBytesWrittenMin = Math.min(taskShuffleBytesWrittenMin, swm.shuffleBytesWritten)
      taskShuffleWriteTimeMin    = Math.min(taskShuffleWriteTimeMin,    swm.shuffleWriteTime   )

      // Max values
      taskDurationMax                = Math.max(taskDurationMax,                ti.duration               )

      taskDiskBytesSpilledMax        = Math.max(taskDiskBytesSpilledMax,        tm.diskBytesSpilled       )
      taskExecutorDeserializeTimeMax = Math.max(taskExecutorDeserializeTimeMax, tm.executorDeserializeTime)
      taskExecutorRunTimeMax         = Math.max(taskExecutorRunTimeMax,         tm.executorRunTime        )
      taskJvmGCTimeMax               = Math.max(taskJvmGCTimeMax,               tm.jvmGCTime              )
      taskMemoryBytesSpilledMax      = Math.max(taskMemoryBytesSpilledMax,      tm.memoryBytesSpilled     )
      taskResultSerializationTimeMax = Math.max(taskResultSerializationTimeMax, tm.resultSerializationTime)
      taskResultSizeMax              = Math.max(taskResultSizeMax,              tm.resultSize             )

      taskFetchWaitTimeMax       = Math.max(taskFetchWaitTimeMax,       srm.fetchWaitTime      )
      taskLocalBlocksFetchedMax  = Math.max(taskLocalBlocksFetchedMax,  srm.localBlocksFetched )
      taskRemoteBlocksFetchedMax = Math.max(taskRemoteBlocksFetchedMax, srm.remoteBlocksFetched)
      taskRemoteBytesReadMax     = Math.max(taskRemoteBytesReadMax,     srm.remoteBytesRead    )
      taskTotalBlocksFetchedMax  = Math.max(taskTotalBlocksFetchedMax,  srm.totalBlocksFetched )

      taskShuffleBytesWrittenMax = Math.max(taskShuffleBytesWrittenMax, swm.shuffleBytesWritten)
      taskShuffleWriteTimeMax    = Math.max(taskShuffleWriteTimeMax,    swm.shuffleWriteTime   )
    }
  }
}

