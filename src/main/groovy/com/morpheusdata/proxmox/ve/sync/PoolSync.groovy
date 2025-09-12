package com.morpheusdata.proxmox.ve.sync

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.CloudPool
import com.morpheusdata.model.Datastore
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.projection.CloudPoolIdentity
import com.morpheusdata.model.projection.DatastoreIdentity
import com.morpheusdata.model.projection.ReferenceDataSyncProjection
import com.morpheusdata.proxmox.ve.ProxmoxVePlugin
import groovy.util.logging.Slf4j
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil

/**
 * @author Neil van Rensburg
 */

@Slf4j
class PoolSync {

    private Cloud cloud
    ProxmoxVePlugin plugin
    private MorpheusContext morpheusContext
    private HttpApiClient apiClient
    private Map authConfig


    public PoolSync(ProxmoxVePlugin plugin, Cloud cloud, HttpApiClient apiClient) {
        this.@plugin = plugin
        this.@cloud = cloud
        this.@morpheusContext = plugin.morpheus
        this.@apiClient = apiClient
        this.@authConfig = plugin.getAuthConfig(cloud)
    }


    def execute() {
    log.debug "PoolSync: Starting pool sync"
        try {
            def listResults = ProxmoxApiComputeUtil.listProxmoxPools(apiClient, plugin.getAuthConfig(cloud))
            log.debug("PoolSync: Pools found: ${listResults?.data}")

            if (listResults?.success) {
                def cloudItems = listResults?.data
                if (cloudItems == null) {
                    log.error("PoolSync: cloudItems is null! listResults=${listResults}")
                }
                def domainRecords = morpheusContext.async.cloud.pool.listIdentityProjections(cloud.id, "proxmox.pool.${cloud.id}", null)
                log.debug "PoolSync: domainRecords=${domainRecords}"

                SyncTask<CloudPoolIdentity, Map, CloudPool> syncTask = new SyncTask<>(domainRecords, cloudItems as Collection)
                syncTask.addMatchFunction { CloudPoolIdentity domainObject, Map cloudItem ->
                    domainObject.externalId == cloudItem.poolid
                }
                .withLoadObjectDetails { List<SyncTask.UpdateItemDto<CloudPoolIdentity, Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<CloudPoolIdentity, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it] }
                    return morpheusContext.async.cloud.pool.listById(updateItems?.collect { it.existingItem.id }).map { CloudPool pool ->
                        new SyncTask.UpdateItem<CloudPool, Map>(existingItem: pool, masterItem: updateItemMap[pool.id].masterItem)
                    }
                }
                .onAdd { itemsToAdd ->
                    addMissingPools(itemsToAdd)
                }
                .onUpdate { List<SyncTask.UpdateItem<CloudPool, Map>> updateItems ->
                    // Implement update logic if needed
                }
                .onDelete { removeItems ->
                    morpheusContext.async.cloud.pool.bulkRemove(removeItems).blockingGet()
                }
                .start()
            } else {
                log.error("PoolSync: listResults.success=false or listResults is null! listResults=${listResults}")
            }
        } catch (e) {
            log.error("PoolSync error: ${e}", e)
            log.error("PoolSync: Environment debug data: cloud=${cloud}, plugin=${plugin}, apiClient=${apiClient}, authConfig=${authConfig}")
        }
    }


    private addMissingPools(Collection<Map> addList) {
        log.info("PoolSync:addMissingPools: addList.size(): ${addList.size()}")
        def poolAdds = []
        try {
            addList?.each { cloudItem ->
                def saveConfig = [
                        owner     : cloud.owner,
                        name       : cloudItem.poolid,
                        externalId : cloudItem.poolid,
                        uniqueId   : cloudItem.poolid,
                        internalId : cloudItem.poolid,
                        refType    : 'ComputeZone',
                        refId      : cloud.id,
                        cloud      : cloud,
                        category   : "proxmox.pool.${cloud.id}",
                        code       : "proxmox.pool.${cloud.id}.${cloudItem.poolid}",
                        active     : true
                ]
                poolAdds << new CloudPool(saveConfig)
            }
            log.info("Adding Pools: $poolAdds")
            morpheusContext.services.cloud.pool.bulkCreate(poolAdds)
        } catch (e) {
            log.error "Error adding Pool ${e}", e
        }
    }

}
