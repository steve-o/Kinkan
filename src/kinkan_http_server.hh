/* HTTP embedded server.
 */

#ifndef KINKAN_HTTP_SERVER_HH_
#define KINKAN_HTTP_SERVER_HH_

#ifdef _WIN32
#	include <winsock2.h>
#endif

#include <string>
#include <memory>
#include <vector>

#include "chromium/basictypes.hh"
#include "chromium/values.hh"
#include "net/server/http_server.hh"
#include "net/server/http_server_request_info.hh"

#ifdef _WIN32           
#	define in_port_t	uint16_t
#endif

class GURL;

namespace kinkan
{
	struct ConsumerInfo {
		ConsumerInfo();
		~ConsumerInfo();

		std::string infrastructure_address;	/* e.g. ads2.6.3.L1.linux.rrg 64-bit */
		std::string infrastructure_version;
		bool is_active;
		unsigned msgs_received;
	};

	struct ProviderInfo {
		ProviderInfo();
		~ProviderInfo();

		std::string hostname;
		std::string username;
		int pid;
		unsigned client_count;	/* all RSSL port connections, active or not */
		unsigned msgs_received; /* all message types including metadata */
	};

	class KinkanHttpServer
		: public net::HttpServer::Delegate
	{
	public:

		class ConsumerDelegate {
		public:
			virtual ~ConsumerDelegate() {}

			virtual void CreateInfo(ConsumerInfo* info) = 0;
		};

		class ProviderDelegate {
		public:
			virtual ~ProviderDelegate() {}

			virtual void CreateInfo(ProviderInfo* info) = 0;
		};

// Constructor doesn't start server.
		explicit KinkanHttpServer (chromium::MessageLoopForIO* message_loop_for_io, ConsumerDelegate* consumer_delegate, ProviderDelegate* provider_delegate);

// Destroys the object.
		virtual ~KinkanHttpServer();

// Starts HTTP server: start listening port |port| for HTTP requests.
		bool Start (in_port_t port);

// Stops HTTP server.
		void Shutdown();

	private:
// net::HttpServer::Delegate methods:
		virtual void OnHttpRequest (int connection_id, const net::HttpServerRequestInfo& info) override;
		virtual void OnWebSocketRequest(int connection_id, const net::HttpServerRequestInfo& info) override;
		virtual void OnWebSocketMessage(int connection_id, const std::string& data) override;
		virtual void OnClose(int connection_id) override;

		void OnJsonRequestUI(int connection_id, const net::HttpServerRequestInfo& info);
		void OnDiscoveryPageRequestUI(int connection_id);
		void OnPollScriptRequestUI(int connection_id);

		void SendJson(int connection_id, net::HttpStatusCode status_code, chromium::Value* value, const std::string& message);

		std::string GetDiscoveryPageHTML() const;
		std::string GetPollScriptJS() const;

// Port for listening.
		in_port_t port_;

// Contains encapsulated object for listening for requests.
		std::shared_ptr<net::HttpServer> server_;

// Message loop to direct all tasks towards.
		chromium::MessageLoopForIO* message_loop_for_io_;

		ConsumerDelegate* consumer_delegate_;
		ProviderDelegate* provider_delegate_;
	};

} /* namespace kinkan */

#endif /* KINKAN_HTTP_SERVER_HH_ */

/* eof */
