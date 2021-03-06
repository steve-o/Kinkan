/* HTTP embedded server.
 */

#include "kinkan_http_server.hh"

#include "chromium/json/json_reader.hh"
#include "chromium/json/json_writer.hh"
#include "chromium/logging.hh"
#include "chromium/strings/stringprintf.hh"
#include "chromium/values.hh"
#include "net/base/ip_endpoint.hh"
#include "net/base/net_errors.hh"
#include "net/server/http_server_response_info.hh"
#include "net/socket/tcp_listen_socket.hh"
#include "url/gurl.hh"

namespace {

#include "index.html.h"
#include "poll.js.h"

}  // namespace

kinkan::ConsumerInfo::ConsumerInfo()
	: is_active (false)
	, msgs_received (0) {
}

kinkan::ConsumerInfo::~ConsumerInfo() {
}

kinkan::ProviderInfo::ProviderInfo()
	: pid (0)
	, client_count (0)
	, msgs_received (0) {
}

kinkan::ProviderInfo::~ProviderInfo() {
}

kinkan::KinkanHttpServer::KinkanHttpServer (
	chromium::MessageLoopForIO* message_loop_for_io,
	chromium::MessageLoop* consumer_message_loop,
	kinkan::KinkanHttpServer::ConsumerDelegate* consumer_delegate,
	kinkan::KinkanHttpServer::ProviderDelegate* provider_delegate
	)
	: port_ (0)
	, consumer_message_loop_ (consumer_message_loop)
	, message_loop_for_io_ (message_loop_for_io)
	, consumer_delegate_ (consumer_delegate)
	, provider_delegate_ (provider_delegate)
{
}

kinkan::KinkanHttpServer::~KinkanHttpServer()
{
	Shutdown();
}

/* Open HTTP port and listen for incoming connection attempts.
 */
bool
kinkan::KinkanHttpServer::Start (
	in_port_t port
	)
{
	if ((bool)server_)
		return true;

	net::TCPListenSocketFactory factory (message_loop_for_io_, "::", port);
	server_ = std::make_shared<net::HttpServer> (factory, this);
	net::IPEndPoint address;

	if (net::OK != server_->GetLocalAddress (&address)) {
		NOTREACHED() << "Cannot start HTTP server";
		return false;
	}

	LOG(INFO) << "Address of HTTP server: " << address.ToString();
	return true;
}

void
kinkan::KinkanHttpServer::Shutdown()
{
	if (!(bool)server_)
		return;

	server_.reset();
}

void
kinkan::KinkanHttpServer::OnHttpRequest (
	int connection_id,
	const net::HttpServerRequestInfo& info
	)
{
	VLOG(1) << "Processing HTTP request: " << info.path;
	if (0 == info.path.find ("/json")) {
		OnJsonRequestUI (connection_id, info);
		return;
	}

	if (info.path == "" || info.path == "/") {
		OnDiscoveryPageRequestUI (connection_id);
		return;
	}
	if (info.path == "/poll.js") {
		OnPollScriptRequestUI (connection_id);
		return;
	}

	if (0 != info.path.find ("/provider/")) {
		server_->Send404 (connection_id);
		return;
	}

	server_->Send404 (connection_id);
}

void
kinkan::KinkanHttpServer::OnWebSocketRequest (
	int connection_id,
	const net::HttpServerRequestInfo& info
	)
{
	server_->AcceptWebSocket(connection_id, info);
}

void
kinkan::KinkanHttpServer::OnWebSocketMessage (
	int connection_id,
	const std::string& data
	)
{
	std::string response;
	std::shared_ptr<chromium::DictionaryValue> dict(new chromium::DictionaryValue);

	if (data == "p") {
		ProviderInfo info;
		provider_delegate_->CreateInfo (&info);
		dict->SetString("hostname", info.hostname);
		dict->SetString("username", info.username);
		dict->SetInteger("pid", info.pid);
		dict->SetInteger("clients", info.client_count);
		dict->SetInteger("provider_msgs", info.msgs_received);
	} else if (data == "c") {
		consumer_message_loop_->PostTask ([this, connection_id]() {
			std::string message;
			std::shared_ptr<chromium::DictionaryValue> dict(new chromium::DictionaryValue);
			ConsumerInfo info;
			consumer_delegate_->CreateInfo (&info);
			dict->SetString("ip", info.ip);
			dict->SetString("component", info.component);
			dict->SetString("app", info.app);
			dict->SetBoolean("is_active", info.is_active);
			dict->SetInteger("consumer_msgs", info.msgs_received);
			chromium::JSONWriter::Write(dict.get(), &message);
			message_loop_for_io_->PostTask ([this, connection_id, message]() {
				server_->SendOverWebSocket(connection_id, message);
			});
		});
		return;
	}
/* default return empty JSON object {} */

	chromium::JSONWriter::Write(dict.get(), &response);
	server_->SendOverWebSocket(connection_id, response);
}

void
kinkan::KinkanHttpServer::OnClose (
	int connection_id
	)
{
}

static bool ParseJsonPath(
    const std::string& path,
    std::string* command,
    std::string* target_id) {
  // Fall back to list in case of empty query.
  if (path.empty()) {
    *command = "list";
    return true;
  }
  if (path.find("/") != 0) {
    // Malformed command.
    return false;
  }
  *command = path.substr(1);
  size_t separator_pos = command->find("/");
  if (separator_pos != std::string::npos) {
    *target_id = command->substr(separator_pos + 1);
    *command = command->substr(0, separator_pos);
  }
  return true;
}

void
kinkan::KinkanHttpServer::OnJsonRequestUI (
	int connection_id,
	const net::HttpServerRequestInfo& info
	)
{
// Trim /json
	std::string path = info.path.substr(5);

// Trim fragment and query
	std::string query;
	size_t query_pos = path.find("?");
	if (query_pos != std::string::npos) {
		query = path.substr(query_pos + 1);
		path = path.substr(0, query_pos);
	}
	size_t fragment_pos = path.find("#");
	if (fragment_pos != std::string::npos)
		path = path.substr(0, fragment_pos);

	std::string command;
	std::string target_id;
	if (!ParseJsonPath(path, &command, &target_id)) {
		SendJson(connection_id, net::HTTP_NOT_FOUND, nullptr, "Malformed query: " + info.path);
		return;
	}

	if ("info" == command) {
		consumer_message_loop_->PostTask ([this, connection_id]() {
			chromium::DictionaryValue dict;
			std::string json;
			ConsumerInfo info;
			consumer_delegate_->CreateInfo (&info);
			dict.SetString("ip", info.ip);
			dict.SetString("component", info.component);
			dict.SetString("app", info.app);
			dict.SetBoolean("is_active", info.is_active);
			dict.SetInteger("consumer_msgs", info.msgs_received);
			chromium::JSONWriter::Write(&dict, &json);
			message_loop_for_io_->PostTask ([this, connection_id, json]() {
				std::unique_ptr<chromium::DictionaryValue> dict (static_cast<chromium::DictionaryValue*>(chromium::JSONReader::Read (json, false)));
				ProviderInfo info;
				provider_delegate_->CreateInfo (&info);
				dict->SetString("hostname", info.hostname);
				dict->SetString("username", info.username);
				dict->SetInteger("pid", info.pid);
				dict->SetInteger("clients", info.client_count);
				dict->SetInteger("provider_msgs", info.msgs_received);
				SendJson(connection_id, net::HTTP_OK, dict.get(), std::string());
			});
		});
		return;
	}

	SendJson(connection_id, net::HTTP_NOT_FOUND, nullptr, "Unknown command: " + command);
}

void
kinkan::KinkanHttpServer::OnDiscoveryPageRequestUI (
	int connection_id
	)
{
	std::string response = GetDiscoveryPageHTML();
	server_->Send200(connection_id, response, "text/html; charset=UTF-8");
}

void
kinkan::KinkanHttpServer::OnPollScriptRequestUI (
	int connection_id
	)
{
	std::string response = GetPollScriptJS();
	server_->Send200(connection_id, response, "application/json; charset=UTF-8");
}

void
kinkan::KinkanHttpServer::SendJson (
	int connection_id,
	net::HttpStatusCode status_code,
	chromium::Value* value,
	const std::string& message
	)
{
// Serialize value and message.
	std::string json_value;
	if (value) {
		chromium::JSONWriter::WriteWithOptions(value,
			chromium::JSONWriter::OPTIONS_PRETTY_PRINT,
			&json_value);
	}
	std::string json_message;
	std::shared_ptr<chromium::Value> message_object(new chromium::StringValue(message));
	chromium::JSONWriter::Write(message_object.get(), &json_message);

	net::HttpServerResponseInfo response(status_code);
	response.SetBody(json_value + message, "application/json; charset=UTF-8");
	server_->SendResponse(connection_id, response);
}

#include "chromium/strings/string_util.hh"

std::string
kinkan::KinkanHttpServer::GetDiscoveryPageHTML() const
{
	ProviderInfo info;
	provider_delegate_->CreateInfo (&info);

	std::string response (WWW_INDEX_HTML);
	ReplaceFirstSubstringAfterOffset (&response, 0, "%HOSTNAME%", info.hostname);
	ReplaceFirstSubstringAfterOffset (&response, 0, "%USERNAME%", info.username);
	ReplaceFirstSubstringAfterOffset (&response, 0, "%PID%", std::to_string (info.pid));
	ReplaceFirstSubstringAfterOffset (&response, 0, "%CLIENTS%", std::to_string (info.client_count));
	ReplaceFirstSubstringAfterOffset (&response, 0, "%PROVIDER_MSGS%", std::to_string (info.msgs_received));

	return response;
}

std::string
kinkan::KinkanHttpServer::GetPollScriptJS() const
{
	return std::string (WWW_POLL_JS);
}

/* eof */
