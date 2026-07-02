// PDFweight.h
//
// PDF weight nominal/up/down calculator, based on the MC PDF set's error treatment (hessian/symhessian/replicas).
//
// The user should first get the PDF set's ErrorType automatically using analysis_utils.get_pdf_errtype(lhaID) 
// by passing the analyzer.lhaid value.
//
// Based on the original TIMBER implementation under TIMBER/Framework/src/PDFweight_uncert.cc, but updated to be 
// more general, i.e., does not rely on hard-coded text files but instead the official PDF set info under CVMFS.
//
// Implementation source: https://arxiv.org/pdf/2203.05506 , section 6.3

#pragma once

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "ROOT/RVec.hxx"
using ROOT::RVec;

class PDFweight {
public:
    PDFweight(std::string errtype, bool ignoreEmptyBranch = false);
    ~PDFweight();
    std::vector<float> eval(RVec<float> LHEPdfWeight);

private:
    bool ignoreEmpty;
    std::string errorType;  // full raw ErrorType string, e.g. "symmhessian+as"
    std::string coreType;   // "core" part before any '+', e.g. "hessian", "symmhessian", "replicas"

};

PDFweight::PDFweight(std::string errtype, bool ignoreEmptyBranch) : errorType(errtype), ignoreEmpty(ignoreEmptyBranch) {

    coreType = errorType.substr(0, errorType.find('+'));

    if (coreType != "hessian" && coreType != "symmhessian" && coreType != "replicas") {
        throw std::runtime_error("Unsupported PDF ErrorType '" + coreType + "' for PDF set, only hessian, symmhessian, and replicas are handled.");
    }

    std::cout << "  ErrorType: " << errorType << " -> using '" << coreType << "' formula" << std::endl;
    if (errorType.find('+') != std::string::npos) {
        // for now just throw warning. in practice, including the extra terms is not going to make a huge impact...
        std::cerr << "  WARNING: ErrorType (" << errorType << ") has appended term(s) beyond the core type (e.g. alphaS)." << std::endl;
    }
}

PDFweight::~PDFweight() {}

std::vector<float> PDFweight::eval(RVec<float> LHEPdfWeight) {
    int size = LHEPdfWeight.size();

    if (size == 0) {
        if (ignoreEmpty) return {1.0, 1.0, 1.0};
        throw std::runtime_error(
            "LHEPdfWeight vector empty. May be a known bug in NanoAOD (see "
            "https://github.com/cms-nanoAOD/cmssw/issues/520). To ignore, set "
            "ignoreEmptyBranch=true in the constructor.");
    }

    float nominal = LHEPdfWeight[0];
    float errPlus, errMinus;

    if (coreType == "hessian") {
        // asymmetric eigenvector pairs: members alternate (+,-) relative to member 0
        if ((size - 1) % 2 != 0)
            throw std::runtime_error(
                "'hessian' ErrorType expects an even number of non-central members (+/- pairs), "
                "got " + std::to_string(size - 1));
        float sumPlus = 0.0, sumMinus = 0.0;
        for (int k = 1; k < size; k += 2) {
            float dPlus = LHEPdfWeight[k] - nominal;
            float dMinus = LHEPdfWeight[k + 1] - nominal;
            sumPlus += std::pow(std::max({dPlus, dMinus, 0.0f}), 2);
            sumMinus += std::pow(std::max({-dPlus, -dMinus, 0.0f}), 2);
        }
        errPlus = std::sqrt(sumPlus);
        errMinus = std::sqrt(sumMinus);

    } else if (coreType == "symmhessian") {
        // symmetric eigenvectors: quadrature sum of (member - central)
        float sumsquares = 0.0;
        for (int ipdf = 1; ipdf < size; ipdf++)
            sumsquares += std::pow(LHEPdfWeight[ipdf] - nominal, 2);
        errPlus = errMinus = std::sqrt(sumsquares);

    } else {  // "replicas"
        float pdfavg = std::accumulate(LHEPdfWeight.begin(), LHEPdfWeight.end(), 0.0f) / size;
        float sumsquares = 0.0;
        for (int ipdf = 0; ipdf < size; ipdf++)
            sumsquares += std::pow(LHEPdfWeight[ipdf] - pdfavg, 2);
        float stddev = std::sqrt(sumsquares / (size - 1));
        errPlus = errMinus = stddev;
        nominal = pdfavg;  // replica-set convention: central = mean of replicas, not member 0
    }

    return {
        nominal,
        std::min(13.0f, nominal + errPlus),
        std::max(-13.0f, nominal - errMinus)
    };
}