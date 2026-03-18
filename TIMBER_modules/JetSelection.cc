#include "ROOT/RVec.hxx"

using namespace ROOT::VecOps; //RVec

//Returns indices of jets passing pt, eta, and mass cuts.
RVec<int> SelectJets(
    const ROOT::VecOps::RVec<float> &pt,
    const ROOT::VecOps::RVec<float> &eta,
    const ROOT::VecOps::RVec<float> &mass,
    float ptCut,
    float etaCut,
    float massCut)
{
    ROOT::VecOps::RVec<int> indices;
    for (size_t i = 0; i < pt.size(); ++i) {
        if (pt[i] > ptCut && std::abs(eta[i]) < etaCut && mass[i] > massCut)
            indices.push_back(i);
    }
    return indices;
}

//Returns indices that appear in both lists
RVec<int> IntersectIndices(RVec<int> a, RVec<int> b) {
    std::unordered_set<int> b_set(b.begin(), b.end());
    RVec<int> out;
    for (auto i : a)
        if (b_set.count(i)) out.push_back(i);
    return out;
}


RVec<int> TruncateIndices(const RVec<int>& indices, size_t maxN) {
    if (indices.size() <= maxN) return indices;
    return RVec<int>(indices.begin(), indices.begin() + maxN);
}