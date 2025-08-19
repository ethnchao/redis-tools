#!/bin/bash

# ./ctrl.sh -a <create|apply|delete|restart|check|deploy> -e <env> -c <cluster>
# -a action，动作
# 支持选项：
#   deploy:  复制编译后的redis-tools-linux到sre-tools pod
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

printHelp() {
    echo "
./ctrl.sh -a <create|apply|delete|restart|check|deploy> -e <env> -c <cluster>
-a action，动作
支持选项：
create:  新建ConfigMap 和 Deployment到集群中
apply:   部署或更新ConfigMap 和 Deployment到集群中
delete:  从集群中删除ConfigMap 和 Deployment
restart: 重启
check:   检查不正常的Pod
deploy:  复制编译后的redis-tools-linux到sre-tools pod
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
    
    # 检查deploy操作需要的redis-tools-linux文件
    if [[ $action == 'deploy' ]]; then
        if [[ ! -f "./redis-tools-linux" ]]; then
            echo "【错误】deploy操作需要redis-tools-linux文件，请先编译生成该文件"
            exit 1
        fi
        echo "【发现】redis-tools-linux文件，准备部署"
    fi
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

# 查找集群中的sre-tools pod并复制redis-tools-linux
deployRedisTools() {
    cluster="$1"
    echo "=========================================="
    echo "🚀 开始在集群 ${cluster} 中部署 redis-tools"
    echo "=========================================="
    
    # 查找default命名空间中label为app=sre-tools且状态为Running的pod
    pods=$(kubectl --context="${cluster}" -n default get pods -l app=sre-tools --field-selector status.phase=Running -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
    
    if [[ -z "${pods}" ]]; then
        echo "❌ 在集群 ${cluster} 的default命名空间中未找到状态为Running的 app=sre-tools pod"
        return 1
    fi
    
    echo "📋 找到以下Running状态的 sre-tools pod: ${pods}"
    
    # 遍历每个pod
    for pod in ${pods}; do
        echo "------------------------------------------"
        echo "🔍 处理 pod: ${pod}"
        
        # 检查pod中是否已存在/opt/redis-tools
        echo "🔍 检查 pod 中是否已存在 /opt/redis-tools"
        if kubectl --context="${cluster}" -n default exec "${pod}" -- test -f /opt/redis-tools >/dev/null 2>&1; then
            echo "📦 检测到已存在的 redis-tools，进行备份"
            
            # 创建备份目录
            kubectl --context="${cluster}" -n default exec "${pod}" -- mkdir -p /opt/bak >/dev/null 2>&1
            
            # 备份现有文件
            if kubectl --context="${cluster}" -n default exec "${pod}" -- cp /opt/redis-tools /opt/bak/redis-tools.bak >/dev/null 2>&1; then
                echo "✅ 成功备份到 /opt/bak/redis-tools.bak"
            else
                echo "⚠️  备份失败，但继续执行复制操作"
            fi
        else
            echo "ℹ️  pod 中不存在 /opt/redis-tools，直接复制"
        fi
        
        # 复制新的redis-tools-linux到pod
        echo "📂 复制 redis-tools-linux 到 pod ${pod}:/opt/redis-tools"
        if kubectl --context="${cluster}" -n default cp ./redis-tools-linux "${pod}":/opt/redis-tools >/dev/null 2>&1; then
            echo "✅ 成功复制 redis-tools-linux 到 pod ${pod}"
            
            # 设置执行权限
            if kubectl --context="${cluster}" -n default exec "${pod}" -- chmod +x /opt/redis-tools >/dev/null 2>&1; then
                echo "✅ 成功设置执行权限"
            else
                echo "⚠️  设置执行权限失败"
            fi
            
            # 验证复制结果
            if kubectl --context="${cluster}" -n default exec "${pod}" -- test -x /opt/redis-tools >/dev/null 2>&1; then
                echo "🎉 验证成功: redis-tools 已成功部署到 pod ${pod}"
            else
                echo "❌ 验证失败: redis-tools 可能未正确部署"
            fi
        else
            echo "❌ 复制失败: 无法将 redis-tools-linux 复制到 pod ${pod}"
        fi
    done
    
    echo "=========================================="
    echo "✅ 集群 ${cluster} 的部署操作完成"
    echo "=========================================="
}

deployRedisToolsWrapper() {
    echo "🚀 开始批量部署 redis-tools 到 sre-tools pod"
    echo "=========================================="
    
    while read -r cluster; do
        if [[ -z $cluster ]]; then
            continue
        fi
        deployRedisTools "${cluster}"
    done < "${EXEC_FILE}"
    
    echo "🎉 所有集群的 redis-tools 部署操作完成！"
}

mainOps() {
    action="$1"
    case $action in
        deploy)
            deployRedisToolsWrapper
            ;;
        *)
            echo "未知的操作: $action"
            echo "支持的操作: create, apply, delete, restart, check, deploy"
            ;;
    esac
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
