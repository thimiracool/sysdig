//
// k8s_namespace_handler.cpp
//

#include "k8s_namespace_handler.h"
#include "sinsp.h"
#include "sinsp_int.h"

// filters normalize state and event JSONs, so they can be processed generically:
// event is turned into a single-entry array, state is turned into an array of ADDED events

std::string k8s_namespace_handler::EVENT_FILTER =
	"{"
	" type: .type,"
	" apiVersion: .object.apiVersion,"
	" kind: .object.kind,"
	" items:"
	" [ .object |"
	"  {"
	"   name: .metadata.name,"
	"   uid: .metadata.uid,"
	"   timestamp: .metadata.creationTimestamp,"
	"   labels: .metadata.labels"
	"  }"
	" ]"
	"}";

std::string k8s_namespace_handler::STATE_FILTER =
	"{"
	" type: \"ADDED\","
	" apiVersion: .apiVersion,"
	" kind: \"Namespace\","
	" items:"
	" ["
	"  .items[] |"
	"  {"
	"   name: .metadata.name,"
	"   uid: .metadata.uid,"
	"   timestamp: .metadata.creationTimestamp,"
	"   labels: .metadata.labels"
	"  }"
	" ]"
	"}";

k8s_namespace_handler::k8s_namespace_handler(
	k8s_state_t& state
#ifdef HAS_CAPTURE
	,ptr_t dependency_handler
	,collector_ptr_t collector
	,std::string url
	,const std::string& http_version
	,ssl_ptr_t ssl
	,bt_ptr_t bt
	,bool connect
	,bool blocking_socket
	,bool set_clusterid
	,bool clusterid_only
#endif // HAS_CAPTURE
	):
		k8s_handler("k8s_namespace_handler", true,
#ifdef HAS_CAPTURE
					url, "/api/v1/namespaces",
					STATE_FILTER, EVENT_FILTER, "", collector,
					http_version, 1000L, ssl, bt, true,
					connect, dependency_handler, blocking_socket,
#endif // HAS_CAPTURE
					~0, &state)
		,m_set_clusterid(set_clusterid)
		,m_clusterid_only(clusterid_only)
{
	ASSERT(!(m_clusterid_only && !m_set_clusterid));
}

k8s_namespace_handler::~k8s_namespace_handler()
{
}

bool k8s_namespace_handler::handle_component(const Json::Value& json, const msg_data* data)
{
	if (data == nullptr)
	{
		throw sinsp_exception("K8s namespace handler: data is null.");
	}
	if (m_state == nullptr)
	{
		throw sinsp_exception("K8s node handler: state is null.");
	}

	if((data->m_reason == k8s_component::COMPONENT_ADDED) ||
	   (data->m_reason == k8s_component::COMPONENT_MODIFIED))
	{
		if (m_set_clusterid && data->m_name == "default")
		{
			// There is always one and only one default namespace
			// per cluster so use its uid as a made-up clusterid
			m_state->set_clusterid(data->m_uid);
		}

		if (!m_clusterid_only)
		{
			k8s_ns_t& ns =
				m_state->get_component<k8s_namespaces, k8s_ns_t>(m_state->get_namespaces(),
										 data->m_name, data->m_uid);

			k8s_pair_list entries = k8s_component::extract_object(json, "labels");
			if(entries.size() > 0)
			{
				ns.set_labels(std::move(entries));
			}
		}
	}
	else if(data->m_reason == k8s_component::COMPONENT_DELETED)
	{
		if (!m_clusterid_only &&
		    !m_state->delete_component(m_state->get_namespaces(), data->m_uid))
		{
			{
				log_not_found(*data);
				return false;
			}
		}
	}
	else if(data->m_reason != k8s_component::COMPONENT_ERROR)
	{
		g_logger.log(std::string("Unsupported K8S " + name() + " event reason: ") +
			     std::to_string(data->m_reason), sinsp_logger::SEV_ERROR);
		return false;
	}

	return true;
}
