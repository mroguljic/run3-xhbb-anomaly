/**
 * Class to implement the factorization and renormalization (QCD) scales at matrix-element level
 * Sources:
 * 	- https://twiki.cern.ch/twiki/bin/viewauth/CMS/TopSystematics#Factorization_and_renormalizatio
 * 	- https://git.rwth-aachen.de/3pia/cms_analyses/common/-/blob/11e0c5225416a580d27718997a11dc3f1ec1e8d1/processor/generator.py#L93
 *	- https://indico.cern.ch/event/494682/contributions/1172505/attachments/1223578/1800218/mcaod-Feb15-2016.pdf
 *	- https://indico.cern.ch/event/938672/contributions/3943718/attachments/2073936/3482265/MC_ContactReport_v3.pdf
 * The LHEScaleWeight branch in NanoAOD can be 8 or 9 indices long depending on the generator.
 * For Factorization uncertainty we are interested in indices (up,down):
 *	- 4, 3 		(8 indices)
 *	- 5, 3		(9 indices)
 * For Renormalization uncertainty we are interested in indices (up,down):
 *	- 6, 1		(8 indices)
 *	- 7, 1		(9 indices)
 * For combined uncertainty we are interested in indices (up,down):
 *	- 7, 0		(8 indices)
 * 	- 8, 0		(9 indices)
 *
 * For the case of 9 indices, we have to normalize by the nominal value (stored in index 4)
 *
 * Each of the three methods returns a vector of floats containing {nom, up, down} variations
 */
#include <ROOT/RVec.hxx>
#include <initializer_list>
using namespace ROOT::VecOps;

class QCDScaleWeight {
    public:
	/*
	 * Each method takes in the LHEScaleWeight branch of the NanoAOD containing the per-event generator weights 
	 * corresponding to variations of the renormalization and factorization scale (μR and μF) at the matrix-element (ME) level.
	 */
	QCDScaleWeight(){};
	~QCDScaleWeight(){};

	//---------------------------------------------------------------------------------------------
	// Functions to creatre the QCD factorization, renormalization, and combined event weights
	//---------------------------------------------------------------------------------------------
	// Function to handle the Factorization scale uncertainties
	RVec<float> evalFactorization(RVec<float> LHEScaleWeights);
	// Function to handle the Renormalization scale uncertainties
	RVec<float> evalRenormalization(RVec<float> LHEScaleWeights);
	// Function to handle the combined variation (simultaneously varying uR and uF)
	RVec<float> evalCombined(RVec<float> LHEScaleWeights);

        //---------------------------------------------------------------------------------------------
        // Functions to create the QCD scale uncertainty. Following the prescription in the final link above,
	    // slide 27
        //---------------------------------------------------------------------------------------------
	RVec<float> evalUncert(RVec<float> LHEScaleWeights);
};

RVec<float> QCDScaleWeight::evalUncert(RVec<float> LHEScaleWeights) {
    int size = LHEScaleWeights.size();
    RVec<float> out(2);
    if (size == 0) {
        throw "LHEScaleWeight vector empty.\n";
    }
    else if (size == 8) {
        float center = LHEScaleWeights[1];      // element 6 also works, both are 1.0
        float deviation = -1.0;
        float scale;
        for (int i : {0,2,3,4,5,6,7}) {
            float temp = abs(center - LHEScaleWeights[i]);
            if (temp > deviation) { deviation = temp; }
        }
        scale = deviation/center;
        out[0] = center + scale;
        out[1] = ((center-scale) > 0) ? center - scale : 0;
    }
    else if (size == 9) {
	float center = LHEScaleWeights[4];      // uR = uF = 1
	float deviation = -1;                   // initialize it to -1                 
	float scale;
	for (int i : {0,1,2,3,5,6,7,8}) {
        // choose the largest of abs(weight - center) as deviation
	    float temp = abs(center-LHEScaleWeights[i]);
	    if (temp > deviation) { deviation = temp; }
	}
	scale = deviation/center;
	out[0] = center + scale;
	out[1] = ((center-scale) > 0) ? center - scale : 0; // prevent neg. weights
    }
    else {
        throw "LHEScaleWeight vector has size other than 0,8,9.\n";
    }
    return out;
}

RVec<float> QCDScaleWeight::evalFactorization(RVec<float> LHEScaleWeights) {
    int size = LHEScaleWeights.size();
    RVec<float> out(3);
    if (size == 0) {
	throw "LHEScaleWeight vector empty.\n";
    }
    // happens for powheg generator (NMSSM signal, for example).
    else if (size == 8) {
        out[0] = 1.0;
        out[1] = LHEScaleWeights[4];
        out[2] = LHEScaleWeights[3];
    }
    // happens for pythia generator. In this case:
    // 	- norm by 'nominal' value LHEScaleWeight[4]
    else if (size == 9) {
        out[0] = 1.0;
        out[1] = LHEScaleWeights[5] / LHEScaleWeights[4];
        out[2] = LHEScaleWeights[3] / LHEScaleWeights[4];
    }
    else { // should never happen
        throw "LHEScaleWeight vector has size other than 0,8,9.\n";
    }
    return out;
};

RVec<float> QCDScaleWeight::evalRenormalization(RVec<float> LHEScaleWeights) {
    int size = LHEScaleWeights.size();
    RVec<float> out(3);
    if (size == 0) {
        throw "LHEScaleWeight vector empty.\n";
    }
    else if (size == 8) {
        out[0] = 1.0;
        out[1] = LHEScaleWeights[6];
        out[2] = LHEScaleWeights[1];	
    }
    else if (size == 9) {
        out[0] = 1.0;
        out[1] = LHEScaleWeights[7] / LHEScaleWeights[4];
        out[2] = LHEScaleWeights[1] / LHEScaleWeights[4];	
    }
    else { // should never happen
        throw "LHEScaleWeight vector has size other than 0,8,9.\n";
    }
    return out;
};

RVec<float> QCDScaleWeight::evalCombined(RVec<float> LHEScaleWeights) {
    int size = LHEScaleWeights.size();
    RVec<float> out(3);
    if (size == 0) {
        throw "LHEScaleWeight vector empty.\n";
    }
    else if (size == 8) {
        out[0] = 1.0;
        out[1] = LHEScaleWeights[7];
        out[2] = LHEScaleWeights[0];
    }
    else if (size == 9) {
        out[0] = 1.0;
        out[1] = LHEScaleWeights[8] / LHEScaleWeights[4];
        out[2] = LHEScaleWeights[0] / LHEScaleWeights[4];
    }
    else { // should never happen
        throw "LHEScaleWeight vector has size other than 0,8,9.\n";
    }
    return out;
};