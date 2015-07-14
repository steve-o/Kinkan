/* User-configurable settings.
 *
 * NB: all strings are locale bound, UPA provides no Unicode support.
 */

#ifndef CONFIG_HH_
#define CONFIG_HH_

#include <string>
#include <sstream>
#include <vector>

namespace kinkan
{

	struct config_t
	{
		config_t();

//  TREP-RT service name, e.g. IDN_RDF, hEDD, ELEKTRON_DD.
		std::string upstream_service_name, downstream_service_name;

//  TREP-RT RSSL hostname or IPv4 address to consume content from.
		std::string rssl_server;

//  TREP-RT RSSL port, e.g. 14002, 14003.
		std::string upstream_rssl_port, downstream_rssl_port;

/* DACS application Id.  If the server authenticates with DACS, the consumer
 * application may be required to pass in a valid ApplicationId.
 * Range: "" (None) or 1-511 as an Ascii string.
 */
		std::string application_id;

//  Name to present for infrastructure amusement.
		std::string application_name;

/* DACS username for consumer, frequently non-checked and set to similar: user1.
 */
		std::string user_name;

/* Typically only enforced for non-interactive providers a unique identifer per
 * process on a given host.
 */
		std::string instance_id;

/* DACS position for consumer, the station which the user is using.
 * Range: "" (None) or "<IPv4 address>/hostname" or "<IPv4 address>/net"
 */
		std::string position;

//  RSSL vendor name presented in directory.
		std::string vendor_name;

//  Client session capacity.
		size_t session_capacity;

//  Symbol map.
		std::string symbol_path;
	};

	inline
	std::ostream& operator<< (std::ostream& o, const config_t& config) {
		std::ostringstream ss;
		o << "config_t: { "
			  "\"upstream_service_name\": \"" << config.upstream_service_name << "\""
			", \"rssl_server\": \"" << config.rssl_server << "\""
			", \"upstream_rssl_port\": \"" << config.upstream_rssl_port << "\""
			", \"downstream_service_name\": \"" << config.downstream_service_name << "\""
			", \"downstream_rssl_port\": \"" << config.downstream_rssl_port << "\""
			", \"application_id\": \"" << config.application_id << "\""
			", \"application_name\": \"" << config.application_name << "\""
			", \"user_name\": \"" << config.user_name << "\""
			", \"instance_id\": \"" << config.instance_id << "\""
			", \"position\": \"" << config.position << "\""
			", \"vendor_name\": \"" << config.vendor_name << "\""
			", \"session_capacity\": " << config.session_capacity << 
			", \"symbol_path\": " << config.symbol_path << 
			" }";
		return o;
	}

} /* namespace kinkan */

#endif /* CONFIG_HH_ */

/* eof */
