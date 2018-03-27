package com.soteradefense.dga.graphx.louvain

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState extends Serializable{

  var community = -1L // 所属社区id
  var communitySigmaTot = 0L // 社区的度数
  var internalWeight = 0L  // 节点总度数
  var nodeWeight = 0L;  // 节点的出度
  var changed = false
   
  override def toString(): String = {
    "{community:"+community+",communitySigmaTot:"+communitySigmaTot+
    ",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+"}"
  }
}