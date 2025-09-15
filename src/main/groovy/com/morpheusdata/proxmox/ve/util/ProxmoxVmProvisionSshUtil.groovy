package com.morpheusdata.proxmox.ve.util

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.model.ComputeServer
import groovy.util.logging.Slf4j

@Slf4j
class ProxmoxVmProvisionSshUtil {

    static String downloadCloudImage(MorpheusContext context, ComputeServer hvNode, String imageUrl) {
        String fileName = new File(imageUrl).getName()
        String remotePath = "/tmp/${fileName}"
        String checkCmd = "[ -f ${remotePath} ] && echo 'exists' || echo 'missing'"
        def result = context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, checkCmd, "", "", "", false, null, true, null, false).blockingGet()
        if (result.output?.trim() == "missing") {
            String downloadCmd = "wget -O ${remotePath} ${imageUrl}"
            context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, downloadCmd, "", "", "", false, null, true, null, false).blockingGet()
        }
        return remotePath
    }

    static String findNextVmid(MorpheusContext context, ComputeServer hvNode) {
        String cmd = "for i in {100..999}; do qm status $i &>/dev/null || { echo $i; break; }; done"
        def result = context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, cmd, "", "", "", false, null, true, null, false).blockingGet()
        return result.output?.trim()
    }

    static void createVm(MorpheusContext context, ComputeServer hvNode, Map params) {
        String vmid = params.vmid
        String vmName = params.vmName
        String vmUser = params.vmUser
        String vmPassword = params.vmPassword
        String vmIp = params.vmIp
        String netmask = params.netmask
        String gateway = params.gateway
        String dns1 = params.dns1
        String dns2 = params.dns2
        String vmStorage = params.vmStorage
        String vmBridge = params.vmBridge
        String vmMemory = params.vmMemory
        String vmCores = params.vmCores
        String vmDiskSize = params.vmDiskSize
        String cloudImagePath = params.cloudImagePath
        String sshKeyContent = params.sshKeyContent

        String createCmd = "qm create ${vmid} --name '${vmName}' --description 'Ubuntu 24.04 Noble - Morpheus' --ostype l26 --cpu cputype=host --cores ${vmCores} --sockets 1 --memory ${vmMemory} --scsihw virtio-scsi-pci --net0 'virtio,bridge=${vmBridge}'"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, createCmd, "", "", "", false, null, true, null, false).blockingGet()

        String importCmd = "qm importdisk ${vmid} ${cloudImagePath} ${vmStorage}"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, importCmd, "", "", "", false, null, true, null, false).blockingGet()

        String bootCmd = "qm set ${vmid} --scsi0 ${vmStorage}:vm-${vmid}-disk-0 --boot c --bootdisk scsi0"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, bootCmd, "", "", "", false, null, true, null, false).blockingGet()

        String resizeCmd = "qm resize ${vmid} scsi0 ${vmDiskSize}"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, resizeCmd, "", "", "", false, null, true, null, false).blockingGet()

        String cloudInitCmd = "qm set ${vmid} --ide2 ${vmStorage}:cloudinit"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, cloudInitCmd, "", "", "", false, null, true, null, false).blockingGet()

        String ipConfigCmd = "qm set ${vmid} --ipconfig0 'ip=${vmIp}/${netmask},gw=${gateway}'"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, ipConfigCmd, "", "", "", false, null, true, null, false).blockingGet()

        String dnsCmd = "qm set ${vmid} --nameserver '${dns1} ${dns2}'"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, dnsCmd, "", "", "", false, null, true, null, false).blockingGet()

        String userCmd = "qm set ${vmid} --ciuser '${vmUser}' --cipassword '${vmPassword}'"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, userCmd, "", "", "", false, null, true, null, false).blockingGet()

        ProxmoxSshUtil.createCustomCloudInit(context, hvNode, vmName, vmUser, vmPassword, sshKeyContent, vmIp, netmask, gateway, dns1, dns2)
        String cicustomCmd = "qm set ${vmid} --cicustom 'user=local:snippets/${vmName}-user.yaml'"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, cicustomCmd, "", "", "", false, null, true, null, false).blockingGet()

        String agentCmd = "qm set ${vmid} --agent enabled=1,fstrim_cloned_disks=1"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, agentCmd, "", "", "", false, null, true, null, false).blockingGet()

        String vgaCmd = "qm set ${vmid} --vga std"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, vgaCmd, "", "", "", false, null, true, null, false).blockingGet()

        String cloudInitUpdateCmd = "qm cloudinit update ${vmid}"
        context.executeSshCommand(hvNode.sshHost, 22, ProxmoxSshUtil.sanitizeSshUsername(hvNode.sshUsername), hvNode.sshPassword, cloudInitUpdateCmd, "", "", "", false, null, true, null, false).blockingGet()
    }
}
