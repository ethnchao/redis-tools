#!/bin/bash

# ./ctrl.sh -a <create|apply|delete|restart|check> -e <env> -c <cluster>
# -a action，动作
# 支持选项：
#   create:  新建ConfigMap 和 Deployment到集群中
#   apply:   部署或更新ConfigMap 和 Deployment到集群中
#   delete:  从集群中删除ConfigMap 和 Deployment
#   restart: 重启
#   check:   检查不正常的Pod
# -e environment，环境类型，是集群的类别，例如prod
# 支持选项：
#   prod, dev, sit, uat, pt
# -c cluster，集群，例如: hd-dev-rke，支持多个，例如：hd-dev-rke,hd-sit-rke
# 有关ConfigMap:
# - 如果有，ConfigMap会优先使用集群名称同名的文件，如果没有则使用默认配置文件
# - 生产环境默认为 ./configmap/prod.yaml
# - 其他环境默认为 ./configmap/test.yaml

PROD_CLUSTER="ali-prod-ack
hd-z-prod-rke
hd-z-prod-tke
hd-z-prod-ack
hd-tob-prod-rke
putuo-prod-rke
putuo-tob-prod-rke"

DEV_CLUSTER="hd-dev-rke
putuo-dev-rke
ali-dev-ack
hd-dev-tke
hd-tob-dev-rke"

SIT_CLUSTER="hd-sit-rke
putuo-sit-rke
hd-sit-ack
hd-sit-tke
hd-tob-sit-rke"

UAT_CLUSTER="hd-uat-rke
putuo-uat-rke
ali-uat-ack
hd-tob-uat-rke
hd-uat-ack
hd-uat-tke
putuo-tob-uat-rke"

PT_CLUSTER="hd-pt-rke
putuo-pt-rke
ali-pt-ack
hd-pt-ack
hd-pt-tke
hd-tob-pt-rke
putuo-training-rke"


# 定义默认值
action=""
environment=""
clusters=""
APPLICATION="redis-tools"
WORK_DIR="/tmp/${APPLICATION}-work-$(date +%s)"
EXEC_FILE="${WORK_DIR}/EXEC"

while getopts ":a:e:c:" opt; do
    case $opt in
        a)
            action=${OPTARG}
            ;;
        e)
            environment=${OPTARG}
            ;;
        c)
            clusters=${OPTARG}
            ;;
        *)
            echo "未知选项: -$OPTARG"
            printHelp
            ;;
    esac
done

printHelp() {
    echo "
./ctrl.sh -a <create|apply|delete|restart|check> -e <env> -c <cluster>
-a action，动作
支持选项：
create:  新建ConfigMap 和 Deployment到集群中
apply:   部署或更新ConfigMap 和 Deployment到集群中
delete:  从集群中删除ConfigMap 和 Deployment
restart: 重启
check:   检查不正常的Pod
-e environment，环境类型，是集群的类别，例如prod
支持选项：
prod, dev, sit, uat, pt
-c cluster，集群，例如: hd-dev-rke，支持多个，例如：hd-dev-rke,hd-sit-rke
有关ConfigMap:
- 如果有，ConfigMap会优先使用集群名称同名的文件，如果没有则使用默认配置文件
- 生产环境默认为 ./configmap/prod.yaml
- 其他环境默认为 ./configmap/test.yaml
"
}

checkVars() {
    if [[ -z ${action} ]]; then
        echo "【错误】没有指定操作类型"
        printHelp
        exit 1
    fi
    echo "【操作】$action"
    if [[ -z ${environment} ]] && [[ -z ${clusters} ]]; then
        echo "【错误】必须至少指定 环境 或 集群 其中之一"
        printHelp
        exit 1
    fi
    if [[ -n ${environment} ]]; then
        echo "【环境类型】${environment}"
    fi
    if [[ -n ${clusters} ]]; then
        echo "【集群】${clusters}"
    fi
    echo "工作&备份目录：${WORK_DIR}"
    mkdir -p "${WORK_DIR}"
}

backup() {
    cluster="$1"
    echo 'Backup...'
    kubectl --context="${cluster}" -n ninja-tasks get deployment "${APPLICATION}" -o yaml > "${WORK_DIR}/${cluster}-ds.yaml"
    kubectl --context="${cluster}" -n monitoring get podmonitor "${APPLICATION}" -o yaml > "${WORK_DIR}/${cluster}-sm.yaml"
}

deploy() {
    action="$1"
    cluster="$2"
    configmap=""
    deployment=""
    if echo "${cluster}" | grep -q "\-prod\-"; then
        configmap=./configmap/prod.yaml
        deployment=./deployment.yaml
    else
        configmap=./configmap/test.yaml
        deployment=./deployment-test.yaml
    fi
    if [[ -f ./configmap/"${cluster}.yaml" ]]; then
        configmap=./configmap/"${cluster}.yaml"
    fi
    echo "部署 ConfigMap: ${configmap}"
    kubectl --context="${cluster}" "${action}" -f ${configmap}
    echo "部署 Deployment: ${deployment}"
    kubectl --context="${cluster}" "${action}" -f ${deployment}
    kubectl --context="${cluster}" "${action}" -f ./podmonitor.yaml
}

deployWrapper() {
    while read -r cluster; do
        if [[ -z $cluster ]]; then
            continue
        fi
        echo "新建 ConfigMap 和 Deployment 到 集群: ${cluster}"
        result=$(kubectl --context="${cluster}" get namespace ninja-tasks -o name 2>/dev/null)
        if [[ -z ${result} ]]; then
            echo "集群中未创建ninja-tasks命名空间，进行创建..."
            kubectl --context="${cluster}" create namespace ninja-tasks
        fi
        deploy "create" "${cluster}"
    done < "${EXEC_FILE}"
    echo '------------------------------------------------'
}

mainOps() {
    action="$1"
    if [[ $action == 'deploy' ]]; then
        deployWrapper
    else
        echo "未知的操作"
    fi
}

main() {
    checkVars
    echo -n > "${EXEC_FILE}"
    if [[ -n ${clusters} ]]; then
        echo "${clusters}" | tr ',' '\n' >> "${EXEC_FILE}"
    fi
    if [[ -n ${environment} ]]; then
        case $environment in
            prod)
                echo "${PROD_CLUSTER}" >> "${EXEC_FILE}"
                ;;
            dev)
                echo "${DEV_CLUSTER}" >> "${EXEC_FILE}"
                ;;
            sit)
                echo "${SIT_CLUSTER}" >> "${EXEC_FILE}"
                ;;
            uat)
                echo "${UAT_CLUSTER}" >> "${EXEC_FILE}"
                ;;
            pt)
                echo "${PT_CLUSTER}" >> "${EXEC_FILE}"
                ;;
            *)
                echo "未知的环境类型: ${environment}"
                printHelp
                ;;
        esac
    fi
    mainOps "${action}"
}

main
