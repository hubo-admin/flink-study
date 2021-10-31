package org.flink.study.entity;

import lombok.Data;

import java.util.Date;

/**
 * Description: 业务背景
 *
 * 随着互联网的发展，网购成为越来越多人的选择，据阿里巴巴财报显示，2020财年阿里巴巴网站成交总额突破一万亿美元，全球年度活跃消费者达9.60亿。
 *
 * 为了满足不同用户的个性化需求，电商平台会根据用户的兴趣爱好推荐合适的商品，从而实现商品排序的千人千面需求。
 * 推荐系统常见的召回路径有U2I（User-Item）、I2I（Item-Item）等。特别的，在推荐场景中，为了更好的提升推荐的时效性与准确性，
 * 平台会基于全网的用户行为信息进行实时的 U2I 及 I2I 的更新，并且基于用户最近的行为信息进行相关性的推荐。
 *
 * 为了获取更多的平台流量曝光，将自己的商品展现在更多的消费者面前，部分商家通过HACK平台的推荐机制从而增加商品的曝光机会。
 * 其中一种典型的手法为“抱大腿”攻击，该方法通过雇佣一批恶意用户协同点击目标商品和爆款商品，从而建立目标商品与爆款商品之间的关联关系，
 * 提升目标商品与爆款商品之间的I2I关联分。商家通过这种方式诱导用户以爆款的心理预期购买名不符实的商品，不仅损害了消费者的利益，降低其购物体验，
 * 还影响了平台和其他商家的信誉，严重扰乱了平台的公平性。实时拦截此类行为，有助于在保证推荐的时效性的同时，保护实时推荐系统不受恶意攻击影响。
 *
 * 如何准确、高效地识别这类型的恶意流量攻击，实时过滤恶意的点击数据是推荐系统中迫切需要解决的问题。
 *
 * 除此之外，此类实时风控系统对数据安全的要求较高。如果系统的拦截算法意外泄漏，HACK平台将得以针对性地加强恶意流量的伪装能力，
 * 增大平台监控恶意流量的难度。因此，此类系统有必要部署在加密的可信环境中。
 *
 * 本赛题要求选手基于Flink，Analytics Zoo/BigDL 等组件，在Occlum环境中搭建保护数据安全的PPML（Privacy Preserving Machine Learning）应用，实现对恶意流量的实时识别。
 **/

/**
 * <p> 用户点击记录 </p>
 *
 * @author hubo
 * @since 2021/10/31 19:58
 */
@Data
public class UserClickRecord {
    private String uuid;        //唯一ID
    private String visitTime;   //该条行为数据的发生时间。实时预测过程中提供的数据的该值基本是单调递增的
    private String userId;      //该条数据对应的用户的id
    private String itemId;      //该条数据对应的商品的id
    private String features;    //该数据的特征，包含N个用空格分隔的浮点数。其中，第1 ~ M个数字代表商品的特征，第M+1 ~ N个数字代表用户的特征
    private String label;       //值为0或1，代表该数据是否为正常行为数据。对于训练数据中无label的数据，这一列的值为-1

    public UserClickRecord(String uuid, String visitTime, String userId, String itemId, String features, String label) {
        this.uuid = uuid;
        this.visitTime = visitTime;
        this.userId = userId;
        this.itemId = itemId;
        this.features = features;
        this.label = label;
    }

    public UserClickRecord(String uuid, String visitTime, String userId, String itemId, String features) {
        this.uuid = uuid;
        this.visitTime = visitTime;
        this.userId = userId;
        this.itemId = itemId;
        this.features = features;
    }

    @Override
    public String toString() {
        return "UserClickRecord{" +
                "uuid='" + uuid + '\'' +
                ", visitTime='" + visitTime + '\'' +
                ", userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", features='" + features + '\'' +
                ", label='" + label + '\'' +
                '}';
    }
}
