/* User-configurable settings.
 */

#include "config.hh"

static const char* kAppName = "Kinkan";
static const char* kDefaultRsslPort = "14002";
static const char* kVendorName = "Thomson Reuters";

kinkan::config_t::config_t() :
/* default values */
	upstream_service_name ("ELEKTRON_EDGE"),
	rssl_server ("nylabats2"),
	upstream_rssl_port (kDefaultRsslPort),
	downstream_service_name ("NOCACHE_VTA"),
	downstream_rssl_port ("24002"),
	application_id (""),
	application_name (kAppName),
	user_name ("user1"),
	instance_id (""),
	position (""),
	vendor_name (kVendorName),
	session_capacity (8)
{
/* C++11 initializer lists not supported in MSVC2010 */
}

/* eof */
