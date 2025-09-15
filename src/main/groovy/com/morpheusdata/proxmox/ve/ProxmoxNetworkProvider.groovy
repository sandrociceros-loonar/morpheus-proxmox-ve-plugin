package com.morpheusdata.proxmox.ve

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.providers.NetworkProvider
import com.morpheusdata.core.providers.CloudInitializationProvider
import com.morpheusdata.core.providers.SecurityGroupProvider
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkServer
import com.morpheusdata.model.NetworkServerType
import com.morpheusdata.model.NetworkSubnet
import com.morpheusdata.model.NetworkType
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

@Slf4j
class ProxmoxNetworkProvider implements NetworkProvider, CloudInitializationProvider {

	public static final String CLOUD_PROVIDER_CODE = 'proxmox-ve.cloud'

	ProxmoxVePlugin plugin
	MorpheusContext morpheus
	SecurityGroupProvider securityGroupProvider

	final String code = 'proxmox-ve.network'
	final String name = 'Proxmox Network'
	final String description = 'Proxmox Network Provider'


	ProxmoxNetworkProvider(ProxmoxVePlugin plugin, MorpheusContext morpheusContext) {
		this.plugin = plugin
		this.morpheus = morpheusContext
	}


	@Override
	String getNetworkServerTypeCode() {
		return 'proxmox-ve.network'
	}

	/**
	 * The CloudProvider code that this NetworkProvider should be attached to.
	 * When this NetworkProvider is registered with Morpheus, all Clouds that match this code will have a
	 * NetworkServer of this type attached to them. Network actions will then be handled via this provider.
	 * @return String Code of the Cloud type
	 */
	@Override
	String getCloudProviderCode() {
		return CLOUD_PROVIDER_CODE
	}

	/**
	 * Provides a Collection of NetworkTypes that can be managed by this provider
	 * @return Collection of NetworkType
	 */
	@Override
	Collection<NetworkType> getNetworkTypes() {
		NetworkType bridgeNetwork = new NetworkType([
				code              : 'proxmox-ve-bridge-network',
				externalType      : 'LinuxBridge',
				cidrEditable      : true,
				dhcpServerEditable: true,
				dnsEditable       : true,
				gatewayEditable   : true,
				vlanIdEditable    : false,
				canAssignPool     : true,
				name              : 'Proxmox VE Bridge Network',
				hasNetworkServer  : true,
				creatable: true
		])

		return [bridgeNetwork]

	}


	@Override
	Collection<OptionType> getOptionTypes() {
		return [
			new OptionType(
				name: 'Bridge de Rede',
				code: 'proxmox.network.bridge',
				fieldName: 'vmBridge',
				fieldLabel: 'Bridge de Rede',
				required: true,
				inputType: 'text',
				defaultValue: 'vmbr0'
			),
			new OptionType(
				name: 'Gateway',
				code: 'proxmox.network.gateway',
				fieldName: 'gateway',
				fieldLabel: 'Gateway',
				required: true,
				inputType: 'text',
				defaultValue: '192.168.0.1'
			),
			new OptionType(
				name: 'DNS Primário',
				code: 'proxmox.network.dns1',
				fieldName: 'dns1',
				fieldLabel: 'DNS Primário',
				required: true,
				inputType: 'text',
				defaultValue: '8.8.8.8'
			),
			new OptionType(
				name: 'DNS Secundário',
				code: 'proxmox.network.dns2',
				fieldName: 'dns2',
				fieldLabel: 'DNS Secundário',
				required: false,
				inputType: 'text',
				defaultValue: '8.8.4.4'
			),
			new OptionType(
				name: 'Netmask',
				code: 'proxmox.network.netmask',
				fieldName: 'netmask',
				fieldLabel: 'Netmask (CIDR)',
				required: true,
				inputType: 'text',
				defaultValue: '24'
			),
			new OptionType(
				name: 'Usuário da VM',
				code: 'proxmox.vm.user',
				fieldName: 'vmUser',
				fieldLabel: 'Usuário da VM',
				required: true,
				inputType: 'text',
				defaultValue: 'ubuntu'
			),
			new OptionType(
				name: 'Senha da VM',
				code: 'proxmox.vm.password',
				fieldName: 'vmPassword',
				fieldLabel: 'Senha da VM',
				required: true,
				inputType: 'password',
				defaultValue: ''
			),
			new OptionType(
				name: 'Chave SSH',
				code: 'proxmox.vm.sshkey',
				fieldName: 'sshKeyContent',
				fieldLabel: 'Chave SSH',
				required: false,
				inputType: 'text',
				defaultValue: ''
			)
		]
	}

	@Override
	Collection<OptionType> getSecurityGroupOptionTypes() {
		return []
	}

	@Override
	ServiceResponse initializeProvider(Cloud cloud) {
		log.info("Initializeing network provider for ${cloud.name}")
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			NetworkServer networkServer = new NetworkServer(
				name: cloud.name,
				type: new NetworkServerType(code:"proxmox-ve.network")
			)
			morpheus.integration.registerCloudIntegration(cloud.id, networkServer).blockingGet()
			rtn.success = true
		} catch (Exception e) {
			rtn.success = false
			log.error("initializeProvider error: {}", e, e)
		}

		return rtn
	}

	@Override
	ServiceResponse deleteProvider(Cloud cloud) {
		log.info("Deleting network provider for ${cloud.name}")
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			// cleanup is done by type, so we do not need to load the record
			// NetworkServer networkServer = morpheusContext.services.networkServer.find(new DataQuery().withFilters([new DataFilter('type.code', "amazon"), new DataFilter('zoneId', cloud.id)]))
			// NetworkServer networkServer = cloud.networkServer // this works too, ha
			NetworkServer networkServer = new NetworkServer(
				name: cloud.name,
				type: new NetworkServerType(code:"proxmox-ve.network")
			)
			morpheus.integration.deleteCloudIntegration(cloud.id, networkServer).blockingGet()
			rtn.success = true
		} catch (Exception e) {
			rtn.success = false
			log.error("deleteProvider error: {}", e, e)
		}

		return rtn
	}

	/**
	 * Creates the Network submitted
	 * @param network Network information
	 * @param opts additional configuration options
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse createNetwork(Network network, Map opts) {
		log.info("NVR: CREATE NETWORK")
		return ServiceResponse.success(network)
	}

	/**
	 * Updates the Network submitted
	 * @param network Network information
	 * @param opts additional configuration options
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse<Network> updateNetwork(Network network, Map opts) {
		log.info("NVR: UPDATE NETWORK")
		return ServiceResponse.success(network)
	}

	/**
	 * Deletes the Network submitted
	 * @param network Network information
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse deleteNetwork(Network network, Map opts) {
		log.info("NVR: DELETE NETWORK")
		return ServiceResponse.success()
	}
	
	@Override
	ServiceResponse createSubnet(NetworkSubnet subnet, Network network, Map opts) {
		log.info("NVR: CREATE SUBNET")
		return ServiceResponse.success()	
	}
	
	@Override
	ServiceResponse updateSubnet(NetworkSubnet subnet, Network network, Map opts) {
		log.info("NVR: UPDATE SUBNET")
		return ServiceResponse.success()	
	}
	
	@Override
	ServiceResponse deleteSubnet(NetworkSubnet subnet, Network network, Map opts) {
		log.info("NVR: DELETE SUBNET")
		return ServiceResponse.success()
	}
	
	@Override
	Collection getRouterTypes() {
		return []
	}



	@Override
	ServiceResponse<Network> prepareNetwork(Network network, Map opts) {
		log.info("NVR: PREPARE NETWORK")
		return ServiceResponse.success(network);
	}


	@Override
	ServiceResponse validateNetwork(Network network, Map opts) {
		log.info("NVR: VALIDATE NETWORK")
		return ServiceResponse.success();
	}


}
