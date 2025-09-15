package com.morpheusdata.proxmox.ve.util

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.LogLevel
import com.morpheusdata.model.TaskResult
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.provisioning.WorkloadRequest
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

/**
 * @author Neil van Rensburg
 */

@Slf4j
class ProxmoxSshUtil {

    static String IMAGE_PATH_PREFIX = "/var/opt/morpheus/morpheus-ui/vms/morpheus-images"
    static String REMOTE_IMAGE_DIR = "/var/lib/vz/template/qemu"

    /**
     * Remove '@pam' from SSH username if present
     */
    static String sanitizeSshUsername(String username) {
        if (username?.endsWith('@pam')) {
            return username.replace('@pam', '')
        }
        return username
    }



    static void createCloudInitDrive(MorpheusContext context, ComputeServer hvNode, WorkloadRequest workloadRequest, String vmId, String datastoreId) {
        log.debug(log.debug("Configuring Cloud-Init"))
        log.debug("Ensuring snippets directory on node: $hvNode.externalId")
        runSshCmd(context, hvNode, "mkdir -p /var/lib/vz/snippets")
        log.debug("Creating cloud-init user-data file on hypervisor node: /var/lib/vz/snippets/$vmId-cloud-init-user-data.yml")
        ProxmoxMiscUtil.sftpCreateFile(hvNode.sshHost, 22, sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, "/var/lib/vz/snippets/$vmId-cloud-init-user-data.yml", workloadRequest.cloudConfigUser, null)
        log.debug("Creating cloud-init user-data file on hypervisor node: /var/lib/vz/snippets/$vmId-cloud-init-network.yml")
        ProxmoxMiscUtil.sftpCreateFile(hvNode.sshHost, 22, sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, "/var/lib/vz/snippets/$vmId-cloud-init-network.yml", workloadRequest.cloudConfigNetwork, null)
        log.debug("Creating cloud-init vm disk: $datastoreId:cloudinit")
        runSshCmd(context, hvNode, "qm set $vmId --ide2 $datastoreId:cloudinit")
        log.debug("Mounting cloud-init data to disk...")
        String ciMountCommand = "qm set $vmId --cicustom \"user=local:snippets/$vmId-cloud-init-user-data.yml,network=local:snippets/$vmId-cloud-init-network.yml\""
        runSshCmd(context, hvNode, ciMountCommand)
    }


    public static String uploadImageAndCreateTemplate(MorpheusContext context, HttpApiClient client, Map authConfig, Cloud cloud, VirtualImage virtualImage, ComputeServer hvNode, String targetDS, String imageFile) {
        def imageExternalId
        def lockKey = "proxmox.ve.imageupload.${cloud.regionCode}.${virtualImage?.id}".toString()
        def lock

        try {
            //hold up to a 1 hour lock for image upload
            lock = context.acquireLock(lockKey, [timeout: 2l * 60l * 1000l, ttl: 2l * 60l * 1000l]).blockingGet()

            //create qcow2 template directory on proxmox
            log.debug("Ensuring Image Directory on node: $hvNode.sshHost")
                def dirOut = context.executeSshCommand(hvNode.sshHost, 22, sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, "mkdir -p $REMOTE_IMAGE_DIR", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
            log.debug("Dir create SSH Task \"mkdir -p $REMOTE_IMAGE_DIR\" results: ${dirOut.toMap().toString()}")

            //sftp .qcow2 file to the directory on proxmox server
            log.debug("uploading Image $IMAGE_PATH_PREFIX/$imageFile to $hvNode.sshHost:$REMOTE_IMAGE_DIR")
                ProxmoxMiscUtil.sftpUpload(hvNode.sshHost, 22, sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, "$IMAGE_PATH_PREFIX/$imageFile", REMOTE_IMAGE_DIR, null)

            //create blank vm template on proxmox
            ServiceResponse templateResp = ProxmoxApiComputeUtil.createImageTemplate(client, authConfig, virtualImage.name, hvNode.externalId, 1, 1024L)
            log.debug("Create Image Template response data $templateResp.data")
            imageExternalId = templateResp.data.templateId

            //import the disk file to the blank vm template
            String fileName = new File("$imageFile").getName()
            log.debug("Executing ImportDisk command on node: qm importdisk $imageExternalId $REMOTE_IMAGE_DIR/$fileName $targetDS")
                def diskCreateOut = context.executeSshCommand(hvNode.sshHost, 22, sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, "qm importdisk $imageExternalId $REMOTE_IMAGE_DIR/$fileName $targetDS", "", "", "", false, LogLevel.info, true, null, false).blockingGet()
            log.debug("Disk ImportDisk SSH Task \"qm importdisk $imageExternalId $REMOTE_IMAGE_DIR/$fileName $targetDS\" results: ${diskCreateOut.toMap().toString()}")

            // Remove disco ide0 se existir (Proxmox anexa por padrão após importdisk)
            def removeIdeCmd = "qm set $imageExternalId --delete ide0"
            log.debug("Removing default ide0 disk if present: $removeIdeCmd")
            def removeIdeOut = context.executeSshCommand(hvNode.sshHost, 22, sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, removeIdeCmd, "", "", "", false, LogLevel.info, true, null, false).blockingGet()
            log.debug("Remove ide0 results: ${removeIdeOut.toMap().toString()}")
            // Detecta nome do disco gerado após importação (LVM Thin geralmente base-<vmid>-disk-0)
            def diskName = "base-$imageExternalId-disk-0"
            // Monta como scsi0 para cloud images Ubuntu
            def diskMountCmd = "qm set $imageExternalId --scsi0 $targetDS:$diskName"
            log.debug("Executing DiskMount SSH Task \"$diskMountCmd\"")
            def diskMountOut = context.executeSshCommand(hvNode.sshHost, 22, sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, diskMountCmd, "", "", "", false, LogLevel.info, true, null, false).blockingGet()
            log.debug("Disk Mount SSH Task \"$diskMountCmd\" results: ${diskMountOut.toMap().toString()}")
            // Define o disco como boot
            def bootOrderCmd = "qm set $imageExternalId --boot order=scsi0"
            log.debug("Setting boot order: $bootOrderCmd")
            def bootOrderOut = context.executeSshCommand(hvNode.sshHost, 22, sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, bootOrderCmd, "", "", "", false, LogLevel.info, true, null, false).blockingGet()
            log.debug("Boot order set results: ${bootOrderOut.toMap().toString()}")
            // Configura BIOS UEFI para cloud images
            def biosCmd = "qm set $imageExternalId --bios ovmf"
            log.debug("Setting BIOS to UEFI: $biosCmd")
            def biosOut = context.executeSshCommand(hvNode.sshHost, 22, sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, biosCmd, "", "", "", false, LogLevel.info, true, null, false).blockingGet()
            log.debug("BIOS set results: ${biosOut.toMap().toString()}")
        } finally {
            context.releaseLock(lockKey, [lock:lock]).blockingGet()
        }
        return imageExternalId
    }


    private static runSshCmd(MorpheusContext context, ComputeServer hvNode, String cmd) {
            String sshUser = sanitizeSshUsername(hvNode.sshUsername)
            TaskResult result = context.executeSshCommand(hvNode.sshHost, 22, sshUser, hvNode.sshPassword, cmd, "", "", "", false, LogLevel.info, true, null, false).blockingGet()
            if (!result.success) {
                throw new Exception(result.toMap())
            }
    }

}
